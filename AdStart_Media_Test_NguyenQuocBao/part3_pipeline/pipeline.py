"""
pipeline.py — Main Prefect flow orchestrating the full ETL.

Run locally:
    python pipeline.py                          # defaults to yesterday
    python pipeline.py --date 2026-01-15        # specific date

Prefect UI (optional):
    prefect server start                        # in another terminal
    python pipeline.py                          # runs and shows in UI

AWS equivalent of each component:
    @flow      → AWS Step Functions state machine
    @task      → individual Lambda function or Glue job step
    retries=3  → Step Functions retry config
    logging    → CloudWatch Logs
    run_id     → Step Functions execution ARN
"""

import argparse
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

from config import DB_PATH, LOG_DIR
from ingest import load_operator_a, load_operator_b, load_operator_c, load_static_files
from transform import (
    build_dim_campaigns,
    build_fct_subscriptions,
    build_fct_billing,
    build_fct_clicks,
    build_mart,
)

# ── Logging setup ───────────────────────────────────────────────
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / f"pipeline_{date.today()}.log"),
    ],
)
logger = logging.getLogger(__name__)


# ── Helpers ─────────────────────────────────────────────────────
def _log_run(conn: duckdb.DuckDBPyConnection, run_id: str, run_date: date,
             step: str, status: str, rows: int = None, error: str = None):
    """
    Write pipeline run status to audit table.
    AWS: replace with boto3 put_item to DynamoDB, or CloudWatch metric.
    """
    conn.execute("""
        INSERT INTO pipeline_runs (run_id, run_date, step, status, rows_processed, error_message, started_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            rows_processed = EXCLUDED.rows_processed,
            error_message = EXCLUDED.error_message,
            finished_at = now()
    """, [run_id, run_date, step, status, rows, error, datetime.now(timezone.utc)])


def get_connection() -> duckdb.DuckDBPyConnection:
    """Single DuckDB connection. AWS: replace with Redshift/Athena client."""
    conn = duckdb.connect(str(DB_PATH))
    # Load schema if first run
    schema_sql = Path(__file__).parent / "schema.sql"
    conn.execute(schema_sql.read_text())
    return conn


# ── Prefect Tasks ───────────────────────────────────────────────
# Each @task maps to one AWS Glue job or Lambda step.
# retries=3 means if the step fails (e.g. file not ready yet), retry up to 3 times.
# retry_delay_seconds=60 means wait 1 minute between retries.

@task(retries=3, retry_delay_seconds=60, name="ingest-operator-a")
def task_ingest_a(run_date: date) -> dict:
    logger = get_run_logger()
    conn = get_connection()
    try:
        result = load_operator_a(conn, run_date)
        logger.info(f"Operator A ingested: {result['row_count']:,} rows")
        return result
    except FileNotFoundError as e:
        logger.error(f"Operator A file missing: {e}")
        raise  # Prefect will retry
    finally:
        conn.close()


@task(retries=3, retry_delay_seconds=60, name="ingest-operator-b")
def task_ingest_b(run_date: date) -> dict:
    logger = get_run_logger()
    conn = get_connection()
    try:
        result = load_operator_b(conn, run_date)
        logger.info(f"Operator B ingested: {result['row_count']:,} rows")
        return result
    finally:
        conn.close()


@task(retries=3, retry_delay_seconds=60, name="ingest-operator-c")
def task_ingest_c(run_date: date) -> dict:
    logger = get_run_logger()
    conn = get_connection()
    try:
        result = load_operator_c(conn, run_date)
        logger.info(f"Operator C ingested: {result['row_count']:,} rows")
        return result
    finally:
        conn.close()


@task(retries=2, retry_delay_seconds=30, name="ingest-static-files")
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


@task(name="data-quality-checks")
def task_quality_checks(run_date: date) -> bool:
    """
    Final validation: assert the mart has expected data.
    AWS: run as a separate Lambda after Glue jobs complete, alert via SNS if failed.
    """
    logger = get_run_logger()
    conn = get_connection()
    failures = []

    try:
        # Check 1: mart has rows for run_date
        mart_rows = conn.execute(
            f"SELECT COUNT(*) FROM mart_daily_performance WHERE report_date = '{run_date}'"
        ).fetchone()[0]
        if mart_rows == 0:
            failures.append(f"mart_daily_performance has 0 rows for {run_date}")

        # Check 2: no negative revenue
        neg_revenue = conn.execute(
            f"SELECT COUNT(*) FROM mart_daily_performance WHERE report_date = '{run_date}' AND total_revenue < 0"
        ).fetchone()[0]
        if neg_revenue > 0:
            failures.append(f"mart has {neg_revenue} rows with negative revenue")

        # Check 3: conversion rates are between 0 and 1
        bad_conv = conn.execute(f"""
            SELECT COUNT(*) FROM mart_daily_performance
            WHERE report_date = '{run_date}'
              AND (sub_conversion_rate > 1 OR bill_conversion_rate > 1)
        """).fetchone()[0]
        if bad_conv > 0:
            failures.append(f"{bad_conv} rows have conversion rate > 100% (data issue)")

        # Check 4: subscriptions <= clicks (basic sanity)
        bad_subs = conn.execute(f"""
            SELECT COUNT(*) FROM mart_daily_performance
            WHERE report_date = '{run_date}'
              AND total_subscriptions > total_clicks
        """).fetchone()[0]
        if bad_subs > 0:
            failures.append(f"{bad_subs} rows where subscriptions > clicks (impossible)")

        if failures:
            for f in failures:
                logger.error(f"Quality check FAILED: {f}")
            # AWS: send SNS alert here
            # sns.publish(TopicArn=ALERT_TOPIC, Message="\n".join(failures))
            raise ValueError(f"Quality checks failed:\n" + "\n".join(failures))

        logger.info(f"All quality checks passed for {run_date}. Mart has {mart_rows} campaign rows.")
        return True

    finally:
        conn.close()


# ── Main Prefect Flow ───────────────────────────────────────────
@flow(
    name="adstart-daily-pipeline",
    description="Daily ETL: operator files → unified facts → mart",
    # AWS Step Functions equivalent: retryAttempts + errorEquals config
)
def daily_pipeline(run_date: date = None):
    """
    Execution order:
    1. Ingest all operator files (parallel where possible)
    2. Load static reference files
    3. Build dim_campaigns
    4. Build fact tables in dependency order
    5. Build mart
    6. Run quality checks

    If ANY step fails, Prefect marks the run as failed and logs the error.
    The run can be retried from the Prefect UI or by re-running this script.
    """
    if run_date is None:
        run_date = date.today() - timedelta(days=1)

    logger.info(f"═══ Pipeline starting for date: {run_date} ═══")

    # Step 1: Ingest (operators can be ingested in parallel — no dependencies)
    # Prefect runs these concurrently if using a ConcurrentTaskRunner
    result_a = task_ingest_a(run_date)
    result_b = task_ingest_b(run_date)
    result_c = task_ingest_c(run_date)
    result_static = task_ingest_static()

    # Step 2: Dimension (depends on campaigns static file)
    dim_count = task_dim_campaigns()

    # Step 3: Facts (depend on dims + raw data)
    sub_count  = task_fct_subscriptions(run_date)
    bill_count = task_fct_billing(run_date)
    click_count = task_fct_clicks(run_date)

    # Step 4: Mart (depends on all facts)
    mart_count = task_mart(run_date)

    # Step 5: Quality checks (last gate before business uses the data)
    quality_ok = task_quality_checks(run_date)

    logger.info(f"""
═══ Pipeline completed for {run_date} ═══
  Subscriptions : {sub_count:,}
  Billings      : {bill_count:,}
  Clicks        : {click_count:,}
  Mart rows     : {mart_count:,}
  Quality       : {'✓ PASSED' if quality_ok else '✗ FAILED'}
""")
    return {"run_date": str(run_date), "mart_rows": mart_count, "quality_ok": quality_ok}


# ── Entry point ─────────────────────────────────────────────────
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
