"""
src/orchestration/pipeline.py — Main Prefect flow orchestrating the full ETL.

Run locally:
    python -m src.orchestration.pipeline                  # yesterday
    python -m src.orchestration.pipeline --date 2026-01-15

Prefect UI (optional):
    prefect server start          # in separate terminal
    python -m src.orchestration.pipeline

AWS equivalents:
    @flow  → Step Functions state machine
    @task  → Lambda function or Glue job step
    retries → Step Functions retry config
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from prefect import flow, task, get_run_logger

from config import configure_logging
from src.utils.db import get_connection
from src.ingest.loaders import (
    load_operator_a,
    load_operator_b,
    load_operator_c,
    load_static_files,
)
from src.transformations import (
    build_dim_campaigns,
    build_fct_subscriptions,
    build_fct_billing,
    build_fct_clicks,
    build_mart,
)
from src.orchestration.quality import run_quality_checks

logger = logging.getLogger(__name__)


# ── Prefect Tasks ────────────────────────────────────────────────
# retries=3: if file not yet delivered, wait and retry (S3 eventual consistency)

@task(retries=3, retry_delay_seconds=60, name="ingest-operator-a")
def task_ingest_a(run_date: date) -> dict:
    conn = get_connection()
    try:
        return load_operator_a(conn, run_date)
    finally:
        conn.close()


@task(retries=3, retry_delay_seconds=60, name="ingest-operator-b")
def task_ingest_b(run_date: date) -> dict:
    conn = get_connection()
    try:
        return load_operator_b(conn, run_date)
    finally:
        conn.close()


@task(retries=3, retry_delay_seconds=60, name="ingest-operator-c")
def task_ingest_c(run_date: date) -> dict:
    conn = get_connection()
    try:
        return load_operator_c(conn, run_date)
    finally:
        conn.close()


@task(retries=2, retry_delay_seconds=30, name="ingest-static")
def task_ingest_static() -> dict:
    conn = get_connection()
    try:
        return load_static_files(conn)
    finally:
        conn.close()


@task(name="build-dim-campaigns")
def task_dim_campaigns() -> int:
    conn = get_connection()
    try:
        return build_dim_campaigns(conn)
    finally:
        conn.close()


@task(name="build-fct-subscriptions")
def task_fct_subscriptions(run_date: date) -> int:
    conn = get_connection()
    try:
        return build_fct_subscriptions(conn, run_date)
    finally:
        conn.close()


@task(name="build-fct-billing")
def task_fct_billing(run_date: date) -> int:
    conn = get_connection()
    try:
        return build_fct_billing(conn, run_date)
    finally:
        conn.close()


@task(name="build-fct-clicks")
def task_fct_clicks(run_date: date) -> int:
    conn = get_connection()
    try:
        return build_fct_clicks(conn, run_date)
    finally:
        conn.close()


@task(name="build-mart")
def task_mart(run_date: date) -> int:
    conn = get_connection()
    try:
        return build_mart(conn, run_date)
    finally:
        conn.close()


@task(name="quality-checks")
def task_quality(run_date: date) -> bool:
    conn = get_connection()
    try:
        return run_quality_checks(conn, run_date)
    finally:
        conn.close()


# ── Main Prefect Flow ────────────────────────────────────────────

@flow(
    name="adstart-daily-pipeline",
    description="Daily ETL: operator files → unified facts → mart",
)
def daily_pipeline(run_date: date | None = None) -> dict:
    """
    Execution order:
      1. Ingest operator files (parallel-safe, no shared state)
      2. Load static reference files
      3. Build dim_campaigns
      4. Build fact tables (sub → billing → clicks, in dependency order)
      5. Build mart
      6. Run quality checks

    Any step failure marks the Prefect run as FAILED and can be retried from the UI.
    """
    if run_date is None:
        run_date = date.today() - timedelta(days=1)

    configure_logging(run_date)
    logger.info(f"═══ Pipeline starting for date: {run_date} ═══")

    # Step 1 — Ingest (can run in parallel with ConcurrentTaskRunner)
    task_ingest_a(run_date)
    task_ingest_b(run_date)
    task_ingest_c(run_date)
    task_ingest_static()

    # Step 2 — Dimensions
    dim_count = task_dim_campaigns()

    # Step 3 — Facts (order matters: sub must precede billing and clicks)
    sub_count   = task_fct_subscriptions(run_date)
    bill_count  = task_fct_billing(run_date)
    click_count = task_fct_clicks(run_date)

    # Step 4 — Mart
    mart_count = task_mart(run_date)

    # Step 5 — Quality gate
    quality_ok = task_quality(run_date)

    logger.info(
        f"\n═══ Pipeline completed for {run_date} ═══\n"
        f"  Subscriptions : {sub_count:,}\n"
        f"  Billings      : {bill_count:,}\n"
        f"  Clicks        : {click_count:,}\n"
        f"  Mart rows     : {mart_count:,}\n"
        f"  Quality       : {'✓ PASSED' if quality_ok else '✗ FAILED'}\n"
    )
    return {
        "run_date":   str(run_date),
        "mart_rows":  mart_count,
        "quality_ok": quality_ok,
    }


# ── Entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run AdStart daily ETL pipeline")
    parser.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        default=date.today() - timedelta(days=1),
        help="Date to process (YYYY-MM-DD). Defaults to yesterday.",
    )
    args = parser.parse_args()
    result = daily_pipeline(run_date=args.date)
    print(f"\nResult: {result}")
