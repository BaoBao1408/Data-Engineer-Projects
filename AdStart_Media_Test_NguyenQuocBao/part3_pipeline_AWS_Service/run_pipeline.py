#!/usr/bin/env python3
"""
run_pipeline.py — Script to run the full pipeline end-to-end.

This is the single entry point for daily operations.
Use this script instead of calling pipeline.py directly to get:
  - Pre-flight checks (AWS credentials, buckets, env vars)
  - Data upload if needed (dev/test mode)
  - Pipeline execution
  - Summary printed to stdout

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  COMPLETE RUN FLOW (read this before running)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  FIRST TIME (run once only):
  ───────────────────────────
  1. Install deps:
       pip install -r requirements_aws.txt

  2. Configure AWS CLI:
       aws configure --profile adstart-dev
       export AWS_PROFILE=adstart-dev

  3. Create AWS resources:
       python infrastructure/setup_aws.py \\
         --account-id $(aws sts get-caller-identity --query Account --output text) \\
         --region eu-west-1

  4. Upload sample data:
       python infrastructure/upload_sample_data.py --date 2026-01-15

  5. Run pipeline:
       python run_pipeline.py --date 2026-01-15

  DAILY (automated or manual):
  ─────────────────────────────
  Manual:
       python run_pipeline.py               # Run for yesterday
       python run_pipeline.py --date 2026-01-20

  Backfill 7 days:
       python run_pipeline.py --backfill-days 7

  Local mode (no AWS required):
       python run_pipeline.py --local --date 2026-01-15

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)-35s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_pipeline")

BANNER = """
╔══════════════════════════════════════════════════════════════╗
║         adstart Data Pipeline — AWS Edition                  ║
║         S3 + Glue + Athena + Prefect                         ║
╚══════════════════════════════════════════════════════════════╝"""


# ── Pre-flight checks ─────────────────────────────────────────────

def check_aws_credentials() -> bool:
    """Verify that AWS credentials are still valid."""
    try:
        import boto3
        from botocore.exceptions import NoCredentialsError, ClientError
        sts = boto3.client("sts")
        identity = sts.get_caller_identity()
        logger.info(
            f"  AWS credentials OK — Account: {identity['Account']}, "
            f"ARN: {identity['Arn'].split('/')[-1]}"
        )
        return True
    except Exception as e:
        logger.error(f"  AWS credentials FAIL: {e}")
        logger.error("  → Run: aws configure --profile adstart-dev")
        logger.error("  → Or set: export AWS_PROFILE=adstart-dev")
        return False


def check_s3_buckets() -> bool:
    """Verify that S3 buckets exist and are accessible."""
    from config.base import settings
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3", region_name=settings.aws_region)
    all_ok = True

    for bucket_name, label in [
        (settings.raw_bucket,           "Raw bucket"),
        (settings.warehouse_bucket,     "Warehouse bucket"),
        (settings.athena_output_bucket, "Athena output bucket"),
    ]:
        try:
            s3.head_bucket(Bucket=bucket_name)
            logger.info(f"  ✓ {label}: s3://{bucket_name}/")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchBucket"):
                logger.error(f"  ✗ {label} NOT FOUND: s3://{bucket_name}/")
                logger.error(f"    → Run: python infrastructure/setup_aws.py --account-id YOUR_ID")
            else:
                logger.error(f"  ✗ {label} error ({code}): {e}")
            all_ok = False

    return all_ok


def check_raw_files_exist(run_date: date) -> bool:
    """Check whether operator files are already present on S3."""
    from config.base import settings
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3", region_name=settings.aws_region)
    all_ok = True

    for op_key, prefix in settings.operator_s3_prefixes.items():
        key = f"{prefix}/date={run_date}/data.csv"
        try:
            s3.head_object(Bucket=settings.raw_bucket, Key=key)
            logger.info(f"  ✓ Found s3://{settings.raw_bucket}/{key}")
        except ClientError:
            logger.warning(f"  ⚠ Not found: s3://{settings.raw_bucket}/{key}")
            all_ok = False

    return all_ok


def preflight_checks(run_date: date, is_aws: bool) -> bool:
    """Run all pre-flight checks. Returns True if all pass."""
    logger.info("[ Pre-flight checks ]")

    if not is_aws:
        logger.info("  LOCAL mode — skipping AWS checks.")
        return True

    ok = True
    ok = check_aws_credentials() and ok
    if ok:
        ok = check_s3_buckets() and ok
    if ok:
        files_ok = check_raw_files_exist(run_date)
        if not files_ok:
            logger.warning(
                "  Some operator files are not yet on S3.\n"
                f"  → Upload with: python infrastructure/upload_sample_data.py --date {run_date}"
            )

    return ok


# ── Upload helper (dev mode) ──────────────────────────────────────

def maybe_upload_data(run_date: date, auto_upload: bool) -> None:
    """Upload sample data if the --upload flag is set."""
    if not auto_upload:
        return
    logger.info("[ Uploading sample data ]")
    from infrastructure.upload_sample_data import (
        upload_operator_files, upload_static_files
    )
    import boto3
    from config.base import settings
    s3 = boto3.client("s3", region_name=settings.aws_region)
    upload_operator_files(s3, settings.raw_bucket, run_date,
                          ["operator_a", "operator_b", "operator_c"])
    upload_static_files(s3, settings.raw_bucket)
    logger.info("  ✓ Sample data uploaded.")


# ── Main runner ───────────────────────────────────────────────────

def run(run_date: date, is_local: bool, backfill_days: int) -> int:
    """
    Run the pipeline and return an exit code (0=success, 1=failure).
    """
    from src.orchestration.pipeline import run_pipeline

    dates = (
        [run_date - timedelta(days=i) for i in range(backfill_days - 1, -1, -1)]
        if backfill_days > 1
        else [run_date]
    )

    all_results = []
    any_failure = False

    for d in dates:
        logger.info(f"\n{'─'*60}")
        logger.info(f"Processing: {d}")
        logger.info(f"{'─'*60}")

        try:
            result = run_pipeline(run_date=d)
            all_results.append(result)
            if result.get("status") != "SUCCESS":
                any_failure = True
        except Exception as exc:
            logger.error(f"Pipeline FAILED for {d}: {exc}")
            all_results.append({"run_date": str(d), "status": "FAILED", "error": str(exc)})
            any_failure = True

    # ── Print summary ─────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  PIPELINE RUN SUMMARY")
    print("═" * 60)
    for r in all_results:
        status_icon = "✓" if r.get("status") == "SUCCESS" else "✗"
        print(f"  {status_icon} {r['run_date']:12s}  {r.get('status', 'UNKNOWN')}")
        if r.get("status") == "SUCCESS":
            ingest = r.get("ingest", {})
            facts  = r.get("facts",  {})
            mart   = r.get("mart",   {})
            print(f"               Ingest : op_a={ingest.get('operator_a')} "
                  f"op_b={ingest.get('operator_b')} "
                  f"op_c={ingest.get('operator_c')}")
            print(f"               Facts  : subs={facts.get('fct_subscriptions')} "
                  f"billing={facts.get('fct_billing')} "
                  f"clicks={facts.get('fct_clicks')}")
            print(f"               Mart   : {mart.get('mart_daily_performance')} campaign rows")
        elif r.get("error"):
            print(f"               Error  : {r['error'][:80]}")

    print("═" * 60)
    print(f"  Total: {len(all_results)} runs | "
          f"Passed: {sum(1 for r in all_results if r.get('status') == 'SUCCESS')} | "
          f"Failed: {sum(1 for r in all_results if r.get('status') != 'SUCCESS')}")
    print("═" * 60)

    if not any_failure and all_results[0].get("status") == "SUCCESS":
        from config.base import settings
        if settings.is_aws:
            print(f"\n  Query results in Athena:")
            print(f"  SELECT * FROM adstart_warehouse.mart_daily_performance")
            print(f"  WHERE report_date = '{all_results[-1]['run_date']}';")

    return 1 if any_failure else 0


# ── CLI ───────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="adstart Data Pipeline — Run from 0 to end",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date", "-d",
        type=lambda s: date.fromisoformat(s),
        default=None,
        help="Run date YYYY-MM-DD (default: yesterday = D-1)",
    )
    parser.add_argument(
        "--backfill-days", "-b",
        type=int,
        default=1,
        help="Process N consecutive days ending at --date (default: 1)",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Force local mode (DuckDB), ignore AWS settings",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload local sample CSV files to S3 before running (dev/test only)",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip pre-flight checks (faster for trusted environments)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    print(BANNER)
    args = parse_args()

    # Resolve run_date
    run_date = args.date or (datetime.now(timezone.utc).date() - timedelta(days=1))
    print(f"\n  Run date     : {run_date}")
    print(f"  Backfill days: {args.backfill_days}")

    # Override env if --local
    if args.local:
        os.environ["PIPELINE_ENV"] = "local"
        print("  Mode         : LOCAL (DuckDB)")
    else:
        env = os.getenv("PIPELINE_ENV", "local")
        print(f"  Mode         : {env.upper()}")

    # Reload settings after potential env change
    import importlib
    import config.base as _cfg_module
    importlib.reload(_cfg_module)
    from config.base import settings

    # Pre-flight checks
    if not args.skip_preflight:
        ok = preflight_checks(run_date, settings.is_aws)
        if not ok and settings.is_aws:
            print("\n  Pre-flight checks FAILED — aborting.")
            print("  Use --skip-preflight to bypass (not recommended).")
            sys.exit(1)
    else:
        logger.info("  Pre-flight checks SKIPPED.")

    # Optional upload
    if args.upload and settings.is_aws:
        maybe_upload_data(run_date, auto_upload=True)

    # Run pipeline
    exit_code = run(
        run_date=run_date,
        is_local=args.local,
        backfill_days=args.backfill_days,
    )

    sys.exit(exit_code)