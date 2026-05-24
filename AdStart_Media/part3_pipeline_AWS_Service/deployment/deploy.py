"""
deployment/deploy.py — Register Prefect deployments with a daily schedule.

How it works:
    A Prefect deployment is the configuration that tells Prefect *how* and *when*
    to run a flow. Once applied, the flow runs automatically on schedule without
    any manual intervention.

Schedule: daily at 06:00 UTC (= 07:00 BST / 13:00 ICT)
    Why 06:00 UTC?
    - Operators typically deliver files at end-of-day or early morning.
    - 06:00 UTC = early morning UK → data is ready before business hours open.
    - ICT (Vietnam): 13:00 → ideal for post-lunch results review.

Prefect version compatibility:
    This file targets Prefect 2.16+.
    - prefect <  2.7  : CronSchedule at prefect.orion.schemas.schedules   (legacy)
    - prefect 2.7-2.15: CronSchedule at prefect.server.schemas.schedules  (old, removed)
    - prefect >= 2.16  : CronSchedule at prefect.client.schemas.schedules  <- used here
    - prefect >= 2.16  : `schedule=` (single object) deprecated -> use `schedules=[]` list
    - flow_runner_kwargs removed in 2.x — do not pass it to build_from_flow

Usage:
    # 1. Start the Prefect server (leave running in one terminal)
    prefect server start

    # 2. Apply both deployments (run in a second terminal)
    python deployment/deploy.py

    # 3. Trigger a manual run for a specific date
    prefect deployment run 'adstart-daily-pipeline/daily' --param run_date=2026-01-15

    # 4. Trigger an on-demand backfill
    prefect deployment run 'adstart-daily-pipeline/backfill' --param run_date=2026-01-01

    # 5. Inspect deployments and flow runs
    prefect deployment ls
    prefect flow-run ls

    # 6. Open the Prefect UI
    open http://127.0.0.1:4200
"""
from __future__ import annotations

from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments import Deployment

from src.orchestration.pipeline import run_pipeline


def create_daily_deployment() -> Deployment:
    """
    Build the production daily deployment.

    Schedule : CronSchedule — 06:00 UTC every day.
    Parameter: run_date=None → pipeline resolves to yesterday (D-1) at runtime.
    Tags     : adstart, daily, production.
    """
    return Deployment.build_from_flow(
        flow=run_pipeline,
        name="daily",
        version="1.0.0",
        work_queue_name="default",
        # Use schedules= (list) — the old schedule= single-object form is
        # deprecated since Prefect 2.16 and silently fails to create the deployment.
        schedules=[
            CronSchedule(
                cron="0 6 * * *",
                timezone="UTC",
            )
        ],
        parameters={
            "run_date": None,   # None -> pipeline defaults to yesterday at runtime
        },
        description=(
            "Production daily ELT pipeline: S3 raw CSV -> Parquet warehouse -> Athena mart. "
            "Processes D-1 data every morning at 06:00 UTC."
        ),
        tags=["adstart", "daily", "production"],
    )


def create_backfill_deployment() -> Deployment:
    """
    Build the on-demand backfill deployment.

    No automatic schedule — triggered manually when historical dates need
    reprocessing. Pass run_date explicitly at trigger time:

        prefect deployment run 'adstart-daily-pipeline/backfill' \\
            --param run_date=2026-01-01
    """
    return Deployment.build_from_flow(
        flow=run_pipeline,
        name="backfill",
        version="1.0.0",
        work_queue_name="default",
        schedules=[],           # No automatic schedule — manual trigger only
        parameters={
            "run_date": None,
        },
        description=(
            "On-demand backfill deployment. "
            "Pass run_date as a parameter when triggering the run."
        ),
        tags=["adstart", "backfill"],
    )


if __name__ == "__main__":
    print("Registering adstart pipeline deployments with Prefect...")
    print()

    daily = create_daily_deployment()
    daily_id = daily.apply()
    print(f"  + Daily deployment applied    ID: {daily_id}")

    backfill = create_backfill_deployment()
    backfill_id = backfill.apply()
    print(f"  + Backfill deployment applied  ID: {backfill_id}")

    print()
    print("Deployments registered.")
    print()
    print("Next steps:")
    print()
    print("  Start the Prefect UI (if not already running):")
    print("    prefect server start")
    print()
    print("  Trigger a manual run for a specific date:")
    print("    prefect deployment run 'adstart-daily-pipeline/daily' \\")
    print("      --param run_date=2026-01-15")
    print()
    print("  Trigger an on-demand backfill:")
    print("    prefect deployment run 'adstart-daily-pipeline/backfill' \\")
    print("      --param run_date=2026-01-01")
    print()
    print("  List all deployments:")
    print("    prefect deployment ls")
    print()
    print("  Open the Prefect UI:")
    print("    open http://127.0.0.1:4200")