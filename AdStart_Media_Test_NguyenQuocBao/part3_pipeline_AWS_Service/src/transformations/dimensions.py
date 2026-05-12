"""
src/transformations/dimensions.py — Build dimension tables.

dim_campaigns: SCD Type 1 upsert từ raw_campaigns.
  - Mới thêm campaign_id chưa có → INSERT
  - Campaign_id đã có → giữ nguyên (không overwrite)

AWS: awswrangler write với mode="overwrite" cho toàn bộ table
     vì dim_campaigns nhỏ (~10-100 rows) — full refresh an toàn.
     Production lớn hơn: dùng dbt snapshot (SCD Type 2).
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, timezone, datetime

import pandas as pd

from src.utils.aws_warehouse import AWSWarehouse

logger = logging.getLogger(__name__)


def build_dim_campaigns(warehouse: AWSWarehouse) -> int:
    """
    Populate dim_campaigns từ raw_campaigns.

    Logic SCD-0 (insert-only):
      - Đọc raw_campaigns (source of truth)
      - Chuẩn hóa cột
      - Ghi vào dim_campaigns (mode=overwrite vì table nhỏ + static)

    AWS mode  : query raw_campaigns từ Athena → write Parquet to dim path
    LOCAL mode: query raw_campaigns từ DuckDB → write vào dim_campaigns table
    """
    df_raw = warehouse.query(
        "SELECT * FROM raw_campaigns",
        layer="raw"
    )

    if df_raw.empty:
        logger.warning("[dim_campaigns] raw_campaigns trống — bỏ qua build.")
        return 0

    # Chuẩn hóa cột
    rename_map = {"id": "campaign_id"} if "id" in df_raw.columns else {}
    df_dim = df_raw.rename(columns=rename_map).copy()

    # Đảm bảo có đúng các cột cần thiết
    required = ["campaign_id", "country", "operator", "service_name",
                "service_model", "partner_id", "status"]
    for col in required:
        if col not in df_dim.columns:
            df_dim[col] = None

    # Chỉ giữ các cột dim
    df_dim = df_dim[required + [c for c in ["created_at"] if c in df_dim.columns]]
    df_dim["loaded_at"] = datetime.now(timezone.utc).isoformat()

    # Loại bỏ duplicates (chỉ giữ campaign_id đầu tiên)
    df_dim = df_dim.drop_duplicates(subset=["campaign_id"], keep="first")

    warehouse.write_table(
        df_dim,
        layer="dimensions",
        table="dim_campaigns",
        partition_cols=[],   # No date partition — static dimension table
        mode="overwrite",
    )

    count = len(df_dim)
    logger.info(f"[dim_campaigns] {count:,} rows total.")
    return count
