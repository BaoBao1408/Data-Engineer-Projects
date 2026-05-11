"""
src/utils/aws_warehouse.py — AWS Storage Layer (thay thế DuckDB).

LOCAL mode  : dùng DuckDB in-memory (giữ nguyên để dev/test không cần AWS)
AWS mode    : dùng awswrangler + S3 + Athena + Glue Catalog

Tại sao awswrangler?
    - Wrapper pandas-native cho S3, Athena, Glue
    - to_parquet() tự động đăng ký table vào Glue Catalog
    - read_sql_query() chạy SQL trên Athena, trả về pandas DataFrame
    - Partition-aware: mode="overwrite_partitions" = idempotent re-run

Pattern chung:
    1. Read  : wr.athena.read_sql_query(sql, database=GLUE_DB) → DataFrame
    2. Write : wr.s3.to_parquet(df, path=S3_PATH, dataset=True,
                                partition_cols=["report_date"],
                                mode="overwrite_partitions",
                                database=GLUE_DB, table=TABLE_NAME)
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# ── Lazy imports (không cần cài boto3 khi chạy local) ───────────

def _get_wr():
    """awswrangler — chỉ import khi AWS mode."""
    try:
        import awswrangler as wr
        return wr
    except ImportError:
        raise ImportError(
            "awswrangler chưa được cài. Chạy: pip install 'awswrangler>=3.5.0'"
        )


def _get_duckdb():
    """DuckDB — dùng cho local mode."""
    import duckdb
    return duckdb


# ── AWSWarehouse class ───────────────────────────────────────────

class AWSWarehouse:
    """
    Facade thống nhất cho cả LOCAL (DuckDB) và AWS (S3+Athena).

    Cách dùng:
        warehouse = AWSWarehouse.from_settings()

        # Write DataFrame → S3 Parquet + đăng ký Glue table
        warehouse.write_table(df, layer="raw", table="raw_operator_a",
                              partition_date=run_date)

        # Query bằng SQL → DataFrame
        df = warehouse.query("SELECT * FROM fct_subscriptions WHERE report_date = '2026-01-15'",
                             layer="facts")

        # Check record count
        n = warehouse.count(table="fct_subscriptions", run_date=run_date, layer="facts")
    """

    def __init__(self, settings):
        from config.base import Environment
        self.settings = settings
        self._is_aws = settings.is_aws
        self._conn = None  # DuckDB connection (local only)

    @classmethod
    def from_settings(cls) -> "AWSWarehouse":
        from config.base import settings
        return cls(settings)

    # ── Connection lifecycle ─────────────────────────────────────

    def open(self) -> "AWSWarehouse":
        if not self._is_aws:
            duckdb = _get_duckdb()
            schema_path = Path(__file__).parents[2] / "schema.sql"
            self._conn = duckdb.connect(str(self.settings.db_path))
            self._conn.execute(schema_path.read_text())
            logger.debug("[warehouse] LOCAL DuckDB connection opened.")
        else:
            logger.debug("[warehouse] AWS mode — Athena connections are stateless.")
        return self

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self.open()

    def __exit__(self, *_):
        self.close()

    # ── S3 path helpers ──────────────────────────────────────────

    def _s3_path(self, layer: str, table: str) -> str:
        """
        layer: "raw" | "dimensions" | "facts" | "mart"
        Trả về: s3://warehouse-bucket/<layer>/<table>/
        """
        s = self.settings
        layer_map = {
            "raw":        s.s3_raw_table_path(table),
            "dimensions": s.s3_dim_path(table),
            "facts":      s.s3_fact_path(table),
            "mart":       s.s3_mart_path(table),
        }
        if layer not in layer_map:
            raise ValueError(f"Unknown layer: {layer}. Chọn: {list(layer_map)}")
        return layer_map[layer]

    def _glue_db(self, layer: str) -> str:
        """Glue database tương ứng với layer."""
        if layer == "raw":
            return self.settings.glue_raw_database
        return self.settings.glue_warehouse_database

    # ── Write ────────────────────────────────────────────────────

    def write_table(
        self,
        df: pd.DataFrame,
        layer: str,
        table: str,
        partition_date: date | None = None,
        partition_cols: list[str] | None = None,
        mode: str = "overwrite_partitions",
    ) -> int:
        """
        Ghi DataFrame lên S3 dưới dạng Parquet, đăng ký vào Glue Catalog.

        AWS mode  : awswrangler.s3.to_parquet() → auto-creates Glue table
        LOCAL mode: INSERT vào DuckDB table tương ứng

        mode="overwrite_partitions" = IDEMPOTENT (xóa partition cũ, ghi lại)
        = tương đương DELETE WHERE report_date=:run_date + INSERT trong DuckDB
        """
        if df.empty:
            logger.warning(f"[{table}] DataFrame trống — bỏ qua ghi.")
            return 0

        if self._is_aws:
            wr = _get_wr()
            s3_path = self._s3_path(layer, table)

            # Partition mặc định theo report_date nếu có
            if partition_cols is None and partition_date is not None:
                df["report_date"] = str(partition_date)
                partition_cols = ["report_date"]

            kwargs: dict[str, Any] = {
                "df":         df,
                "path":       s3_path,
                "dataset":    True,
                "mode":       mode,
                "database":   self._glue_db(layer),
                "table":      table,
                "boto3_session": self._boto3_session(),
            }
            if partition_cols:
                kwargs["partition_cols"] = partition_cols

            wr.s3.to_parquet(**kwargs)
            n = len(df)
            logger.info(f"[{table}] {n:,} rows → s3://{self.settings.warehouse_bucket}/{layer}/{table}/")
            return n

        else:
            # LOCAL: insert vào DuckDB
            return self._local_write(df, table, partition_date)

    def _local_write(self, df: pd.DataFrame, table: str, partition_date: date | None) -> int:
        """LOCAL mode: xóa partition cũ rồi append vào DuckDB."""
        if partition_date:
            self._conn.execute(
                f"DELETE FROM {table} WHERE report_date = '{partition_date}'"
            )
        self._conn.register("_tmp_df", df)
        self._conn.execute(f"INSERT INTO {table} SELECT * FROM _tmp_df")
        return len(df)

    # ── Query ────────────────────────────────────────────────────

    def query(self, sql: str, layer: str = "warehouse") -> pd.DataFrame:
        """
        Chạy SQL và trả về DataFrame.

        AWS mode  : Athena (serverless) — tính phí theo bytes scanned
        LOCAL mode: DuckDB in-memory
        """
        if self._is_aws:
            wr = _get_wr()
            db = self._glue_db(layer)
            return wr.athena.read_sql_query(
                sql=sql,
                database=db,
                s3_output=self.settings.athena_s3_output,
                boto3_session=self._boto3_session(),
            )
        else:
            return self._conn.execute(sql).df()

    def execute(self, sql: str) -> None:
        """Execute SQL không cần kết quả (chỉ LOCAL mode)."""
        if not self._is_aws:
            self._conn.execute(sql)
        else:
            logger.warning("execute() không dùng trong AWS mode. Dùng query() hoặc write_table().")

    def count(self, table: str, run_date: date, layer: str = "facts") -> int:
        """Đếm số rows trong partition của một ngày."""
        sql = f"SELECT COUNT(*) AS n FROM {table} WHERE report_date = '{run_date}'"
        df = self.query(sql, layer=layer)
        return int(df["n"].iloc[0])

    # ── boto3 session ────────────────────────────────────────────

    def _boto3_session(self):
        """Tạo boto3 session với region đúng."""
        import boto3
        return boto3.Session(region_name=self.settings.aws_region)


# ── Convenience factory functions ────────────────────────────────

def get_warehouse() -> AWSWarehouse:
    """Factory function — dùng thay cho get_connection() của DuckDB cũ."""
    wh = AWSWarehouse.from_settings()
    return wh.open()