"""
src/transformations/dimensions.py — Build dimension tables.

dim_campaigns: SCD Type 1 upsert from raw_campaigns.
  - New campaign_id not yet present → INSERT
  - Existing campaign_id → keep as-is (no overwrite)

AWS: awswrangler write with mode="overwrite" for the entire table
     because dim_campaigns is small (~10-100 rows) — full refresh is safe.
     For larger production tables: use dbt snapshot (SCD Type 2).
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
    Populate dim_campaigns from raw_campaigns.

    SCD-0 logic (insert-only):
      - Read raw_campaigns (source of truth)
      - Normalise columns
      - Write to dim_campaigns (mode=overwrite — table is small & static)

    AWS mode  : query raw_campaigns from Athena → write Parquet to dim path
    LOCAL mode: query raw_campaigns from DuckDB → write to dim_campaigns table
    """
    df_raw = warehouse.query(
        "SELECT * FROM raw_campaigns",
        layer="raw"
    )

    if df_raw.empty:
        logger.warning("[dim_campaigns] raw_campaigns is empty — skipping build.")
        return 0

    # Normalise columns
    rename_map = {"id": "campaign_id"} if "id" in df_raw.columns else {}
    df_dim = df_raw.rename(columns=rename_map).copy()

    # Ensure all required columns are present
    required = ["campaign_id", "country", "operator", "service_name",
                "service_model", "partner_id", "status"]
    for col in required:
        if col not in df_dim.columns:
            df_dim[col] = None

    # Keep only dim columns
    df_dim = df_dim[required + [c for c in ["created_at"] if c in df_dim.columns]]
    df_dim["loaded_at"] = datetime.now(timezone.utc).isoformat()

    # Drop duplicates (keep first occurrence of each campaign_id)
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