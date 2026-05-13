"""
src/ingest/validator.py — Pre-write data quality checks on raw DataFrames.

LOCAL mode : checks run directly against the in-memory pandas DataFrame.
AWS mode   : same logic — checks run after reading from S3, before writing
             Parquet to the warehouse, so bad data is caught at ingestion
             time rather than silently propagating into downstream tables.

Production alternatives worth knowing:
    - AWS Glue Data Quality (Glue DQ rules) — fully managed, no code required,
      integrates natively with Glue ETL jobs.
    - Great Expectations on Glue — code-based, version-controlled rule sets,
      generates HTML data docs as a side effect.

Both alternatives offer the same two core constraint types implemented here:
    RowCountConstraint    -> min_row_count check
    CompletenessConstraint -> max_null_rate check per column
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from config.base import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Required columns per raw table
#
# These columns must not exceed the null-rate threshold defined in settings
# (default: 5%). Any column listed here that is missing from the DataFrame
# altogether is also reported as a quality warning.
#
# Note on raw_campaigns: the source file uses "id" as the primary key column
# name, but loaders.py renames it to "campaign_id" before calling validate.
# The entry below reflects the post-rename column name.
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "raw_operator_a": ["transaction_id", "event_code", "msisdn", "event_time"],
    "raw_operator_b": ["transaction_id", "transaction_type", "msisdn", "created_at"],
    "raw_operator_c": ["message_id", "tracking_code", "msisdn", "received_time"],
    "raw_campaigns":  ["campaign_id", "operator", "service_name", "partner_id"],
    "raw_clicks":     ["rotate_id", "campaign_id", "clicked_at"],
}


def validate_dataframe(
    df: pd.DataFrame,
    table_name: str,
    required_cols: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run row-count and null-rate checks against a raw staging DataFrame.

    The function always returns a result dict so the pipeline can log quality
    metadata regardless of whether warnings were raised. It raises ValueError
    only for hard failures (empty file) that must stop the pipeline entirely.

    Args:
        df            : The DataFrame to validate, already cast to target types.
        table_name    : Used for log messages and as the fallback key into
                        REQUIRED_COLUMNS when required_cols is not supplied.
        required_cols : Explicit list of column names to check for nulls.
                        Defaults to REQUIRED_COLUMNS[table_name] if omitted.

    Returns:
        {
            "table":    str,        # table_name
            "row_count": int,       # total rows loaded
            "warnings": list[str],  # quality warnings (empty list if clean)
        }

    Raises:
        ValueError: if row_count < settings.min_row_count (empty or missing file).

    AWS Glue DQ equivalents:
        RowCountConstraint(minRows=1)
        CompletenessConstraint("column_name", min_completeness=0.95)
    """
    cols = required_cols if required_cols is not None else REQUIRED_COLUMNS.get(table_name, [])

    # -- Row count check -----------------------------------------------------
    row_count = len(df)
    if row_count < settings.min_row_count:
        raise ValueError(
            f"[{table_name}] Source file is empty or missing — "
            f"got {row_count} rows (minimum: {settings.min_row_count}). "
            "Pipeline halted."
        )

    # -- Null-rate checks per required column --------------------------------
    warnings: list[str] = []

    for col in cols:
        if col not in df.columns:
            warnings.append(
                f"Required column '{col}' is absent from the DataFrame."
            )
            continue

        # Count both NaN values and strings that are blank after stripping.
        null_count = int(
            df[col].isna().sum()
            + (df[col].astype(str).str.strip() == "").sum()
        )
        null_rate = null_count / row_count

        if null_rate > settings.max_null_rate:
            warnings.append(
                f"Column '{col}': {null_rate:.1%} null or empty values "
                f"(threshold: {settings.max_null_rate:.0%}, "
                f"affected rows: {null_count:,})"
            )

    if warnings:
        logger.warning(
            "[%s] Quality warnings:\n  %s",
            table_name,
            "\n  ".join(warnings),
        )

    logger.info("[%s] %s rows loaded — quality checks passed.", table_name, f"{row_count:,}")

    return {
        "table":     table_name,
        "row_count": row_count,
        "warnings":  warnings,
    }