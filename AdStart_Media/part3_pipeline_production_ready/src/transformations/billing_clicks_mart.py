"""
src/transformations/billing_clicks_mart.py

Builds:
  - fct_billing   — billing events from all 3 operators
  - fct_clicks    — click funnel enriched with page_events flags
  - mart_daily_performance — pre-aggregated BI mart (Layer 2: includes unattributed columns)
"""
from __future__ import annotations

import logging
from datetime import date

import duckdb

from src.utils.db import run_sql_file

logger = logging.getLogger(__name__)


# ── Billing ──────────────────────────────────────────────────────

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
    Enrich raw_clicks with boolean conversion-funnel flags from page_events.
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
    Layer 2: mart now includes unattributed_subscriptions, unattributed_revenue_est,
    and attribution_rate so BI dashboards show the full economic picture.

    AWS: dbt run --select mart_daily_performance, or Redshift materialized view.
    """
    run_sql_file(conn, "mart/mart_daily_performance.sql", {"run_date": run_date})

    count = conn.execute(
        f"SELECT COUNT(*) FROM mart_daily_performance WHERE report_date = '{run_date}'"
    ).fetchone()[0]

    # Log attributed vs estimated total revenue for ops visibility
    revenue_row = conn.execute(f"""
        SELECT
            ROUND(SUM(total_revenue), 2)              AS attributed_revenue,
            ROUND(SUM(unattributed_revenue_est), 2)   AS unattributed_est,
            ROUND(SUM(unattributed_subscriptions), 0) AS unattributed_subs
        FROM mart_daily_performance
        WHERE report_date = '{run_date}'
    """).fetchone()

    attr_rev, unattr_rev, unattr_subs = revenue_row or (0, 0, 0)

    logger.info(
        f"[mart_daily_performance] {count:,} rows inserted for {run_date}. "
        f"Attributed revenue: £{attr_rev:,.2f} | "
        f"Unattributed est: £{unattr_rev:,.2f} (from {int(unattr_subs or 0)} unattributed subs)"
    )
    return count
