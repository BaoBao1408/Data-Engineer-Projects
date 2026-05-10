"""
src/transformations/billing.py — Build fct_billing.
src/transformations/clicks.py  — Build fct_clicks.
src/transformations/mart.py    — Build mart_daily_performance.

Each module contains one public function that:
  - Delegates SQL to the appropriate sql/facts/ or sql/mart/ file
  - Returns a row count for audit/logging
"""
# billing.py content is in this module; split into separate files if preferred.

from __future__ import annotations
import logging
from datetime import date
import duckdb
from temp.src.utils.db import run_sql_file

logger = logging.getLogger(__name__)


# ── Billing ─────────────────────────────────────────────────────

def build_fct_billing(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Merge billing events from all 3 operators.
    is_first_bill + billing_sequence computed via window functions in SQL.
    IDEMPOTENCY: each SQL file deletes its own operator's rows before inserting.
    """
    params = {"run_date": run_date}
    run_sql_file(conn, "facts/fct_billing_operator_a.sql", params)
    run_sql_file(conn, "facts/fct_billing_operator_b.sql", params)
    run_sql_file(conn, "facts/fct_billing_operator_c.sql", params)

    count = conn.execute(
        f"SELECT COUNT(*) FROM fct_billing WHERE report_date = '{run_date}'"
    ).fetchone()[0]
    logger.info(f"[fct_billing] {count:,} rows inserted for {run_date}.")
    return count


# ── Clicks ───────────────────────────────────────────────────────

def build_fct_clicks(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Enrich raw_clicks with boolean conversion-funnel flags.
    All aggregations pre-computed for fast BI dashboard queries.
    """
    run_sql_file(conn, "facts/fct_clicks.sql", {"run_date": run_date})
    count = conn.execute(
        f"SELECT COUNT(*) FROM fct_clicks WHERE report_date = '{run_date}'"
    ).fetchone()[0]
    logger.info(f"[fct_clicks] {count:,} rows inserted for {run_date}.")
    return count


# ── Mart ─────────────────────────────────────────────────────────

def build_mart(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Pre-aggregate all metrics into mart_daily_performance.
    This is the primary table consumed by BI tools (Metabase, Looker).
    AWS: dbt run --select mart_daily_performance, or Redshift materialized view.
    """
    run_sql_file(conn, "mart/mart_daily_performance.sql", {"run_date": run_date})
    count = conn.execute(
        f"SELECT COUNT(*) FROM mart_daily_performance WHERE report_date = '{run_date}'"
    ).fetchone()[0]
    logger.info(f"[mart_daily_performance] {count:,} rows inserted for {run_date}.")
    return count
