"""
ETL Pipeline – Medallion Architecture (Bronze → Silver → Gold).

Bronze : Raw data as-is from ingestion (stored in raw/ zone)
Silver : Cleaned, validated, typed, deduplicated
Gold   : Aggregated, business-ready datasets for analytics / Power BI

Flow:
    Source Files → [Ingestion] → Bronze
    Bronze → [ETL/Transform] → Silver
    Silver → [ETL/Aggregate] → Gold → [Loader] → Warehouse (Azure SQL)
"""
import hashlib
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import pandas as pd
from loguru import logger

from src.ingestion.connectors.azure_blob_connector import get_data_lake
from src.ingestion.connectors.sql_connector import get_warehouse
from src.quality.validators.schema_validator import DataQualityValidator


class PipelineContext:
    """Carries state and lineage through pipeline stages."""

    def __init__(self, pipeline_name: str, run_id: str, source_file: str):
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.source_file = source_file
        self.started_at = datetime.now(timezone.utc)
        self.stages_completed: list[str] = []
        self.metrics: dict[str, Any] = {}
        self.errors: list[str] = []

    def record_stage(self, stage: str, rows_in: int, rows_out: int) -> None:
        self.stages_completed.append(stage)
        self.metrics[stage] = {
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_dropped": rows_in - rows_out,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        logger.info(
            f"[{self.run_id}] {stage}: {rows_in} → {rows_out} rows "
            f"(dropped {rows_in - rows_out})"
        )

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "source_file": self.source_file,
            "started_at": self.started_at.isoformat(),
            "stages": self.stages_completed,
            "metrics": self.metrics,
            "errors": self.errors,
        }


class DocumentETLPipeline:
    """
    End-to-end ETL pipeline for document-sourced data.

    Stages:
        1. bronze_ingest   – Store raw data to Data Lake (raw zone)
        2. silver_clean    – Clean, validate, standardize schema
        3. gold_aggregate  – Business aggregations + KPI derivations
        4. warehouse_load  – Insert/upsert into Azure SQL / PostgreSQL
    """

    def __init__(self):
        self.data_lake = get_data_lake()
        self.warehouse = get_warehouse()
        self.validator = DataQualityValidator()

    # ─── Stage 1: Bronze ─────────────────────────────────────────────────────

    def bronze_ingest(
        self,
        df: pd.DataFrame,
        ctx: PipelineContext,
        file_name: str,
    ) -> pd.DataFrame:
        """
        Store raw DataFrame to Data Lake raw zone as Parquet.
        Add lineage columns.
        """
        rows_in = len(df)

        # Lineage metadata
        df = df.copy()
        df["_source_file"] = file_name
        df["_run_id"] = ctx.run_id
        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_pipeline"] = ctx.pipeline_name

        # Persist to Data Lake raw zone
        parquet_buf = df.to_parquet(index=False)
        blob_name = f"{ctx.pipeline_name}/bronze/{ctx.run_id}/{file_name}.parquet"
        self.data_lake.upload_bytes(
            parquet_buf, blob_name, zone="raw", content_type="application/parquet"
        )

        ctx.record_stage("bronze_ingest", rows_in, len(df))
        return df

    # ─── Stage 2: Silver ─────────────────────────────────────────────────────

    def silver_clean(
        self,
        df: pd.DataFrame,
        ctx: PipelineContext,
        schema: Optional[dict] = None,
    ) -> pd.DataFrame:
        """
        Clean and validate data:
        - Drop fully-null rows
        - Trim string whitespace
        - Standardize column names
        - Type casting
        - Deduplication
        - Schema validation
        """
        rows_in = len(df)
        df = df.copy()

        # 1. Drop internal lineage cols for processing
        meta_cols = [c for c in df.columns if c.startswith("_")]
        df_clean = df.drop(columns=meta_cols, errors="ignore")

        # 2. Drop fully-null rows
        df_clean = df_clean.dropna(how="all")

        # 3. Strip string whitespace
        str_cols = df_clean.select_dtypes(include="object").columns
        for col in str_cols:
            df_clean[col] = df_clean[col].astype(str).str.strip()
            df_clean[col] = df_clean[col].replace(
                {"nan": None, "None": None, "": None}
            )

        # 4. Standardize column names
        df_clean.columns = [
            c.lower().strip().replace(" ", "_").replace("-", "_")
            for c in df_clean.columns
        ]

        # 5. Deduplicate
        before_dedup = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        dupes_removed = before_dedup - len(df_clean)
        if dupes_removed:
            logger.info(f"Removed {dupes_removed} duplicate rows")

        # 6. Schema validation (if schema provided)
        if schema:
            validation_result = self.validator.validate(df_clean, schema)
            if not validation_result.passed:
                ctx.errors.extend(validation_result.errors)
                logger.warning(
                    f"Schema validation: {len(validation_result.errors)} issues"
                )

        # 7. Restore lineage cols
        for col in meta_cols:
            df_clean[col] = df[col].values[: len(df_clean)]

        df_clean["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()

        # Persist to processed zone
        parquet_buf = df_clean.to_parquet(index=False)
        blob_name = (
            f"{ctx.pipeline_name}/silver/{ctx.run_id}/cleaned.parquet"
        )
        self.data_lake.upload_bytes(
            parquet_buf, blob_name, zone="processed",
            content_type="application/parquet"
        )

        ctx.record_stage("silver_clean", rows_in, len(df_clean))
        return df_clean

    # ─── Stage 3: Gold ───────────────────────────────────────────────────────

    def gold_aggregate(
        self,
        df: pd.DataFrame,
        ctx: PipelineContext,
        aggregations: Optional[list[Callable]] = None,
    ) -> pd.DataFrame:
        """
        Apply business aggregations and derive KPIs.
        Each aggregation is a callable: df → df.
        """
        rows_in = len(df)
        df = df.copy()

        if aggregations:
            for agg_fn in aggregations:
                try:
                    df = agg_fn(df)
                    logger.info(f"Applied aggregation: {agg_fn.__name__}")
                except Exception as e:
                    logger.error(f"Aggregation {agg_fn.__name__} failed: {e}")
                    ctx.errors.append(f"Aggregation error: {e}")

        df["_gold_processed_at"] = datetime.now(timezone.utc).isoformat()

        # Persist to curated zone
        parquet_buf = df.to_parquet(index=False)
        blob_name = f"{ctx.pipeline_name}/gold/{ctx.run_id}/gold.parquet"
        self.data_lake.upload_bytes(
            parquet_buf, blob_name, zone="curated",
            content_type="application/parquet"
        )

        ctx.record_stage("gold_aggregate", rows_in, len(df))
        return df

    # ─── Stage 4: Load ───────────────────────────────────────────────────────

    def warehouse_load(
        self,
        df: pd.DataFrame,
        ctx: PipelineContext,
        target_table: str,
        schema: str = "public",
        upsert_key: Optional[str] = None,
    ) -> int:
        """
        Load Gold data into the data warehouse.
        Supports append or upsert mode.
        """
        rows_in = len(df)

        # Drop internal lineage cols before loading
        load_df = df.loc[
            :, ~df.columns.str.startswith("_")
        ].copy()

        if upsert_key:
            rows_loaded = self._upsert(load_df, target_table, schema, upsert_key)
        else:
            rows_loaded = self.warehouse.bulk_insert_df(
                load_df, target_table, schema, if_exists="append"
            )

        ctx.record_stage("warehouse_load", rows_in, rows_loaded)
        return rows_loaded

    def _upsert(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str,
        key_col: str,
    ) -> int:
        """
        PostgreSQL upsert via ON CONFLICT DO UPDATE.
        """
        cols = list(df.columns)
        col_defs = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != key_col])
        placeholders = ", ".join([f":{c}" for c in cols])
        col_names = ", ".join(cols)

        sql = f"""
            INSERT INTO {schema}.{table} ({col_names})
            VALUES ({placeholders})
            ON CONFLICT ({key_col}) DO UPDATE SET {col_defs}
        """
        records = df.to_dict(orient="records")
        return self.warehouse.execute_many(sql, records)

    # ─── Run Full Pipeline ────────────────────────────────────────────────────

    def run(
        self,
        df: pd.DataFrame,
        pipeline_name: str,
        source_file: str,
        target_table: str,
        schema: str = "public",
        upsert_key: Optional[str] = None,
        silver_schema: Optional[dict] = None,
        gold_aggregations: Optional[list[Callable]] = None,
    ) -> PipelineContext:
        """Execute the full Bronze → Silver → Gold → Load pipeline."""
        run_id = hashlib.md5(
            f"{pipeline_name}{source_file}{datetime.now().isoformat()}".encode()
        ).hexdigest()[:12]

        ctx = PipelineContext(pipeline_name, run_id, source_file)
        logger.info(f"Starting pipeline [{pipeline_name}] run_id={run_id}")

        try:
            df = self.bronze_ingest(df, ctx, source_file)
            df = self.silver_clean(df, ctx, schema=silver_schema)
            df = self.gold_aggregate(df, ctx, aggregations=gold_aggregations)
            self.warehouse_load(df, ctx, target_table, schema, upsert_key)

            duration = (
                datetime.now(timezone.utc) - ctx.started_at
            ).total_seconds()
            logger.info(
                f"Pipeline [{pipeline_name}] completed in {duration:.2f}s. "
                f"Errors: {len(ctx.errors)}"
            )
        except Exception as exc:
            ctx.errors.append(str(exc))
            logger.error(f"Pipeline [{pipeline_name}] FAILED: {exc}")
            raise

        return ctx
