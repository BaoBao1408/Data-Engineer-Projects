"""
src/ingest/validator.py — Data quality checks on freshly loaded staging tables.

Called after every raw load. Returns a result dict; raises ValueError on critical failures.
AWS: equivalent checks run as Glue DQ rules or Great Expectations suites.
"""
from __future__ import annotations

import logging

import duckdb

from config.base import settings

logger = logging.getLogger(__name__)

# Key columns that MUST NOT be null (column names match staging table, not raw CSV)
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "raw_operator_a": ["transaction_id", "event_code", "msisdn", "event_time"],
    "raw_operator_b": ["transaction_id", "transaction_type", "msisdn", "created_at"],
    "raw_operator_c": ["message_id", "tracking_code", "msisdn", "received_time"],
    "raw_campaigns":  ["id", "operator", "service_name", "partner_id"],
    "raw_clicks":     ["rotate_id", "campaign_id", "clicked_at"],
}


def validate_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    required_cols: list[str] | None = None,
) -> dict:
    """
    Run row-count and null-rate checks on a staging table.
    Returns a result dict. Raises ValueError if any critical threshold is breached.
    """
    cols = required_cols or REQUIRED_COLUMNS.get(table, [])

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if row_count < settings.min_row_count:
        raise ValueError(f"[{table}] Empty file — {row_count} rows. Pipeline aborted.")

    warnings: list[str] = []
    for col in cols:
        try:
            null_count = conn.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE "{col}" IS NULL
                   OR TRIM(CAST("{col}" AS VARCHAR)) = ''
            """).fetchone()[0]
            null_rate = null_count / row_count
            if null_rate > settings.max_null_rate:
                warnings.append(
                    f"Column '{col}': {null_rate:.1%} null/empty "
                    f"(threshold {settings.max_null_rate:.0%})"
                )
        except Exception as exc:
            warnings.append(f"Column '{col}' check error: {exc}")

    if warnings:
        logger.warning(f"[{table}] Quality warnings:\n  " + "\n  ".join(warnings))

    logger.info(f"[{table}] Loaded {row_count:,} rows — checks passed.")
    return {"table": table, "row_count": row_count, "warnings": warnings}
