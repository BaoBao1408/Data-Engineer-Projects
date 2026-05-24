"""
src/transformations/subscriptions.py — Build fct_subscriptions + fct_unattributed_events.

Attribution logic per operator:
  - operator_A: direct rotate_id → campaign_id
  - operator_B: direct rotate_id → campaign_id (SUB rows only)
  - operator_C: tracking_code → raw_tracking_codes → rotate_id → campaign_id

Unattributed operator_C rows are written to fct_unattributed_events (Layer 1)
and counted/logged — NOT silently dropped.
"""
from __future__ import annotations

import logging
from datetime import date

import duckdb

from src.utils.db import run_sql_file

logger = logging.getLogger(__name__)


def build_fct_subscriptions(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Merge subscription events from all 3 operators into fct_subscriptions.
    Unattributed operator_C rows are quarantined into fct_unattributed_events.
    IDEMPOTENCY: each SQL file deletes its own operator's rows for run_date before inserting.
    """
    params = {"run_date": run_date}

    run_sql_file(conn, "facts/fct_subscriptions_operator_a.sql", params)
    run_sql_file(conn, "facts/fct_subscriptions_operator_b.sql", params)
    run_sql_file(conn, "facts/fct_subscriptions_operator_c.sql", params)

    # Layer 1 — quarantine unattributed operator_C rows
    run_sql_file(conn, "facts/fct_unattributed_operator_c.sql", params)
    _log_attribution_summary(conn, run_date)

    count = conn.execute(
        f"SELECT COUNT(*) FROM fct_subscriptions WHERE report_date = '{run_date}'"
    ).fetchone()[0]
    logger.info(f"[fct_subscriptions] {count:,} rows inserted for {run_date}.")
    return count


def _log_attribution_summary(conn: duckdb.DuckDBPyConnection, run_date: date) -> None:
    """
    Log a breakdown of attributed vs unattributed operator_C rows.
    Surfaced as WARNING so ops teams can triage without reading the warehouse.
    """
    # Total DELIVERED rows for the date
    total = conn.execute(f"""
        SELECT COUNT(*) FROM raw_operator_c
        WHERE delivery_status = 'DELIVERED'
          AND _loaded_date = '{run_date}'
    """).fetchone()[0]

    if total == 0:
        return

    # Quarantine breakdown by reason
    rows = conn.execute(f"""
        SELECT unattributed_reason, COUNT(*) AS cnt
        FROM fct_unattributed_events
        WHERE operator     = 'operator_C'
          AND report_date  = '{run_date}'
        GROUP BY unattributed_reason
        ORDER BY cnt DESC
    """).fetchall()

    unattributed = sum(r[1] for r in rows)
    attributed   = total - unattributed
    rate         = attributed / total * 100

    if unattributed > 0:
        breakdown = ", ".join(f"{reason}: {cnt}" for reason, cnt in rows)
        logger.warning(
            f"[fct_subscriptions] operator_C attribution for {run_date}: "
            f"{attributed}/{total} attributed ({rate:.1f}%) — "
            f"quarantined {unattributed} row(s) [{breakdown}]."
        )
    else:
        logger.info(
            f"[fct_subscriptions] operator_C attribution for {run_date}: "
            f"{attributed}/{total} (100%) — no unattributed rows."
        )
