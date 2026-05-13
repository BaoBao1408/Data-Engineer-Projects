"""
src/orchestration/pipeline.py — Prefect flow for the adstart data pipeline.

LOCAL  : DuckDB + local CSV files
AWS    : S3 (raw CSV) → S3 Parquet (warehouse) + Glue Catalog + Athena

Flow stages:
  1. ingest_raw       → Load 3 operators + 4 static files
  2. build_dimensions → dim_campaigns
  3. build_facts      → fct_subscriptions, fct_billing, fct_clicks
  4. build_mart       → mart_daily_performance
  5. quality_checks   → Assertions, SNS alert on failure

Idempotency:
  - Each stage uses mode="overwrite_partitions" → safe to re-run
  - Prefect retry decorator → auto-retry on transient S3/Athena errors

Scheduling:
  AWS production: EventBridge rule → trigger Lambda → kick Prefect run
  Local dev     : `python -m src.orchestration.pipeline --date 2026-01-15`
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from prefect import flow, task, get_run_logger

from config.base import settings
from src.ingest.loaders import (
    load_operator_a, load_operator_b, load_operator_c, load_static_files
)
from src.transformations.dimensions import build_dim_campaigns
from src.transformations.subscriptions import build_fct_subscriptions
from src.transformations.billing_clicks_mart import (
    build_fct_billing, build_fct_clicks, build_mart_daily_performance
)
from src.orchestration.quality import run_quality_checks
from src.utils.aws_warehouse import AWSWarehouse

logger = logging.getLogger(__name__)


# ── Tasks ─────────────────────────────────────────────────────────

@task(
    name="ingest-operator-a",
    retries=3,
    retry_delay_seconds=30,
    description="Load operator_A CSV → S3 Parquet raw_operator_a",
)
def task_load_operator_a(wh: AWSWarehouse, run_date: date) -> dict:
    return load_operator_a(wh, run_date)


@task(
    name="ingest-operator-b",
    retries=3,
    retry_delay_seconds=30,
    description="Load operator_B CSV → S3 Parquet raw_operator_b",
)
def task_load_operator_b(wh: AWSWarehouse, run_date: date) -> dict:
    return load_operator_b(wh, run_date)


@task(
    name="ingest-operator-c",
    retries=3,
    retry_delay_seconds=30,
    description="Load operator_C CSV → S3 Parquet raw_operator_c",
)
def task_load_operator_c(wh: AWSWarehouse, run_date: date) -> dict:
    return load_operator_c(wh, run_date)


@task(
    name="ingest-static-files",
    retries=2,
    description="Load campaigns, clicks, tracking_codes, page_events",
)
def task_load_static_files(wh: AWSWarehouse) -> dict:
    return load_static_files(wh)


@task(name="build-dim-campaigns", description="Populate dim_campaigns (SCD-0)")
def task_build_dim_campaigns(wh: AWSWarehouse) -> int:
    return build_dim_campaigns(wh)


@task(
    name="build-fct-subscriptions",
    description="Merge 3-operator subscriptions with attribution",
    retries=2,
    retry_delay_seconds=60,
)
def task_build_fct_subscriptions(wh: AWSWarehouse, run_date: date) -> int:
    return build_fct_subscriptions(wh, run_date)


@task(name="build-fct-billing", description="Build fct_billing from operator A+B")
def task_build_fct_billing(wh: AWSWarehouse, run_date: date) -> int:
    return build_fct_billing(wh, run_date)


@task(name="build-fct-clicks", description="Build fct_clicks with funnel flags")
def task_build_fct_clicks(wh: AWSWarehouse, run_date: date) -> int:
    return build_fct_clicks(wh, run_date)


@task(name="build-mart", description="Aggregate mart_daily_performance")
def task_build_mart(wh: AWSWarehouse, run_date: date) -> int:
    return build_mart_daily_performance(wh, run_date)


@task(
    name="quality-checks",
    description="Run post-build quality assertions",
)
def task_quality_checks(wh: AWSWarehouse, run_date: date) -> dict:
    suite = run_quality_checks(wh, run_date)
    if not suite.passed:
        raise ValueError(
            f"Quality checks FAILED for {run_date}:\n{suite.summary()}"
        )
    return {
        "total":   len(suite.results),
        "passed":  sum(1 for r in suite.results if r.passed),
        "failed":  len(suite.failures),
    }


# ── Main flow ─────────────────────────────────────────────────────

@flow(
    name="adstart-daily-pipeline",
    description=(
        "Daily ELT pipeline: S3 raw CSV → Parquet warehouse → Athena mart. "
        "Runs for a single business date."
    ),
    log_prints=True,
)
def run_pipeline(run_date: date | None = None) -> dict:
    """
    Main Prefect flow.

    Args:
        run_date: Date to process. Defaults to yesterday (D-1).

    Returns:
        Summary dict with row counts for each layer.

    Usage (local):
        python -m src.orchestration.pipeline --date 2026-01-15

    Usage (Prefect UI):
        Create a deployment → schedule daily at 06:00 UTC
    """
    plog = get_run_logger()

    if run_date is None:
        from datetime import datetime, timezone
        run_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    plog.info(f"════════════════════════════════════════")
    plog.info(f" Pipeline starting: run_date = {run_date}")
    plog.info(f" Environment      : {settings.env.value}")
    if settings.is_aws:
        plog.info(f" Raw bucket       : s3://{settings.raw_bucket}/")
        plog.info(f" Warehouse bucket : s3://{settings.warehouse_bucket}/")
        plog.info(f" Athena DB (raw)  : {settings.glue_raw_database}")
        plog.info(f" Athena DB (wh)   : {settings.glue_warehouse_database}")
    plog.info(f"════════════════════════════════════════")

    # ── Open warehouse connection ─────────────────────────────────
    wh = AWSWarehouse.from_settings().open()

    summary: dict = {"run_date": str(run_date)}

    try:
        # ── Stage 1: Ingest ───────────────────────────────────────
        plog.info("[ Stage 1/5 ] Ingesting raw files ...")
        r_a      = task_load_operator_a(wh, run_date)
        r_b      = task_load_operator_b(wh, run_date)
        r_c      = task_load_operator_c(wh, run_date)
        r_static = task_load_static_files(wh)

        summary["ingest"] = {
            "operator_a":     r_a.get("row_count"),
            "operator_b":     r_b.get("row_count"),
            "operator_c":     r_c.get("row_count"),
            "raw_campaigns":  r_static.get("raw_campaigns", {}).get("row_count"),
            "raw_clicks":     r_static.get("raw_clicks", {}).get("row_count"),
        }
        plog.info(f"  ✓ Ingest: {summary['ingest']}")

        # ── Stage 2: Dimensions ───────────────────────────────────
        plog.info("[ Stage 2/5 ] Building dimension tables ...")
        n_dim = task_build_dim_campaigns(wh)
        summary["dimensions"] = {"dim_campaigns": n_dim}
        plog.info(f"  ✓ dim_campaigns: {n_dim:,} rows")

        # ── Stage 3: Facts ────────────────────────────────────────
        plog.info("[ Stage 3/5 ] Building fact tables ...")
        n_subs    = task_build_fct_subscriptions(wh, run_date)
        n_billing = task_build_fct_billing(wh, run_date)
        n_clicks  = task_build_fct_clicks(wh, run_date)

        summary["facts"] = {
            "fct_subscriptions": n_subs,
            "fct_billing":       n_billing,
            "fct_clicks":        n_clicks,
        }
        plog.info(f"  ✓ Facts: {summary['facts']}")

        # ── Stage 4: Mart ─────────────────────────────────────────
        plog.info("[ Stage 4/5 ] Building mart tables ...")
        n_mart = task_build_mart(wh, run_date)
        summary["mart"] = {"mart_daily_performance": n_mart}
        plog.info(f"  ✓ mart_daily_performance: {n_mart:,} campaign rows")

        # ── Stage 5: Quality ──────────────────────────────────────
        plog.info("[ Stage 5/5 ] Running quality checks ...")
        qr = task_quality_checks(wh, run_date)
        summary["quality"] = qr
        plog.info(f"  ✓ Quality: {qr['passed']}/{qr['total']} checks passed")

        summary["status"] = "SUCCESS"
        plog.info(f"════════════════════════════════════════")
        plog.info(f" Pipeline COMPLETED: {run_date} ✓")
        plog.info(f"════════════════════════════════════════")

    except Exception as exc:
        summary["status"] = "FAILED"
        summary["error"]  = str(exc)
        plog.error(f"Pipeline FAILED for {run_date}: {exc}")
        raise

    finally:
        wh.close()

    return summary


# ── CLI entrypoint ────────────────────────────────────────────────

def _parse_args():
    parser = argparse.ArgumentParser(description="Run adstart data pipeline for a specific date.")
    parser.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        default=None,
        help="Run date ISO format YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=0,
        help="Re-process last N days (including run_date)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.backfill_days > 0:
        run_date = args.date or date.today() - timedelta(days=1)
        dates_to_process = [
            run_date - timedelta(days=i)
            for i in range(args.backfill_days - 1, -1, -1)
        ]
        print(f"Backfill mode: processing {len(dates_to_process)} dates: "
              f"{dates_to_process[0]} → {dates_to_process[-1]}")
        for d in dates_to_process:
            result = run_pipeline(run_date=d)
            print(f"  {d}: {result['status']}")
    else:
        result = run_pipeline(run_date=args.date)
        sys.exit(0 if result["status"] == "SUCCESS" else 1)