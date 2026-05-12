"""
src/ingest/validator.py — Data quality checks on raw DataFrames.

LOCAL : kiểm tra trực tiếp trên pandas DataFrame
AWS   : cùng logic — chạy sau khi read từ S3, trước khi write Parquet

AWS production alternative:
    - AWS Glue Data Quality (Glue DQ rules) — managed, no code
    - Great Expectations on Glue — code-based, version-controlled
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from config.base import settings

logger = logging.getLogger(__name__)

# ── Required columns per table ────────────────────────────────────
# Những column này KHÔNG ĐƯỢC null quá threshold
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "raw_operator_a": ["transaction_id", "event_code", "msisdn", "event_time"],
    "raw_operator_b": ["transaction_id", "transaction_type", "msisdn", "created_at"],
    "raw_operator_c": ["message_id", "tracking_code", "msisdn", "received_time"],
    "raw_campaigns":  ["id", "operator", "service_name", "partner_id"],
    "raw_clicks":     ["rotate_id", "campaign_id", "clicked_at"],
}


def validate_dataframe(
    df: pd.DataFrame,
    table_name: str,
    required_cols: list[str] | None = None,
) -> dict[str, Any]:
    """
    Chạy row-count + null-rate checks trên một DataFrame thô.

    Returns result dict. Raises ValueError nếu vi phạm critical threshold.

    AWS Glue DQ equivalent:
        - RowCountConstraint(minRows=1)
        - CompletenessConstraint("column_X", 0.95)
    """
    cols = required_cols or REQUIRED_COLUMNS.get(table_name, [])

    # ── Row count check ──────────────────────────────────────────
    row_count = len(df)
    if row_count < settings.min_row_count:
        raise ValueError(
            f"[{table_name}] File trống — {row_count} rows. Pipeline dừng."
        )

    # ── Null rate checks ─────────────────────────────────────────
    warnings: list[str] = []
    for col in cols:
        if col not in df.columns:
            warnings.append(f"Column '{col}' không tồn tại trong DataFrame.")
            continue
        null_count = df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum()
        null_rate  = null_count / row_count
        if null_rate > settings.max_null_rate:
            warnings.append(
                f"Column '{col}': {null_rate:.1%} null/empty "
                f"(threshold {settings.max_null_rate:.0%})"
            )

    if warnings:
        logger.warning(f"[{table_name}] Quality warnings:\n  " + "\n  ".join(warnings))

    logger.info(f"[{table_name}] Loaded {row_count:,} rows — checks passed.")
    return {"table": table_name, "row_count": row_count, "warnings": warnings}
