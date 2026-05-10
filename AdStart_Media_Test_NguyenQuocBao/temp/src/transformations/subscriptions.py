"""
src/transformations/subscriptions.py — Build fct_subscriptions.

Attribution logic per operator:
  - operator_A: direct rotate_id → campaign_id
  - operator_B: direct rotate_id → campaign_id (SUB rows only)
  - operator_C: tracking_code → raw_tracking_codes → rotate_id → campaign_id

Unattributed operator_C rows are counted and logged, NOT silently dropped.
"""
from __future__ import annotations

import logging
from datetime import date

import duckdb

from temp.src.utils.db import run_sql_file

logger = logging.getLogger(__name__)


def build_fct_subscriptions(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Merge subscription events from all 3 operators.
    IDEMPOTENCY: each SQL file deletes its own operator's rows for run_date before inserting.
    """
    params = {"run_date": run_date}

    run_sql_file(conn, "facts/fct_subscriptions_operator_a.sql", params)
    run_sql_file(conn, "facts/fct_subscriptions_operator_b.sql", params)
    run_sql_file(conn, "facts/fct_subscriptions_operator_c.sql", params)

    _log_unattributed_operator_c(conn, run_date)

    count = conn.execute(
        f"SELECT COUNT(*) FROM fct_subscriptions WHERE report_date = '{run_date}'"
    ).fetchone()[0]
    logger.info(f"[fct_subscriptions] {count:,} rows inserted for {run_date}.")
    return count


def _log_unattributed_operator_c(conn: duckdb.DuckDBPyConnection, run_date: date) -> None:
    """
    Count and log operator_C rows that could not be attributed.
    These are DELIVERED rows whose tracking_code either:
      - is > 3 chars (SMS parser bug), or
      - has no matching entry in raw_tracking_codes within the validity window.
    """
    unattr = conn.execute(f"""
        SELECT COUNT(*) FROM raw_operator_c oc
        LEFT JOIN raw_tracking_codes tc
            ON  tc.code = oc.tracking_code
            AND oc.received_time BETWEEN tc.created_at AND tc.expired_at
        WHERE oc.delivery_status = 'DELIVERED'
          AND oc._loaded_date    = '{run_date}'
          AND tc.rotate_id IS NULL
    """).fetchone()[0]

    if unattr > 0:
        logger.warning(
            f"[fct_subscriptions] {unattr} operator_C DELIVERED rows unattributed "
            f"for {run_date} — expired or missing tracking_code."
        )
