"""
deployment/deploy.py — Deploy Prefect flow với daily schedule.

Cách hoạt động:
    Prefect deployment = cấu hình "chạy flow này như thế nào + khi nào".
    Sau khi deploy, flow tự chạy theo schedule mà không cần can thiệp thủ công.

Schedule: Hàng ngày lúc 06:00 UTC (= 07:00 BST / 13:00 ICT)
    Tại sao 06:00 UTC?
    - Operators thường deliver file vào cuối ngày hoặc đầu buổi sáng
    - 06:00 UTC = sáng sớm UK → data sẵn sàng trước giờ làm việc
    - ICT (Vietnam): 13:00 → lý tưởng để review kết quả sau bữa trưa

Usage:
    # Start Prefect server (local)
    prefect server start

    # Deploy (trong terminal khác)
    python deployment/deploy.py

    # Trigger thủ công
    prefect deployment run 'adstart-daily-pipeline/daily'
    prefect deployment run 'adstart-daily-pipeline/daily' --param run_date=2026-01-15

    # Xem trạng thái
    prefect deployment ls
    prefect flow-run ls
"""
from __future__ import annotations

from datetime import timedelta
from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from src.orchestration.pipeline import run_pipeline


def create_deployment() -> Deployment:
    """
    Tạo Prefect deployment với:
      - CronSchedule: 06:00 UTC hàng ngày
      - Default parameter: run_date = None (= hôm qua)
      - Retry: 2 lần, delay 5 phút
    """
    deployment = Deployment.build_from_flow(
        flow=run_pipeline,
        name="daily",                          # deployment name
        version="1.0.0",
        work_queue_name="default",
        schedule=CronSchedule(
            cron="0 6 * * *",                  # 06:00 UTC hàng ngày
            timezone="UTC",
        ),
        parameters={
            "run_date": None,                  # None = hôm qua
        },
        description=(
            "Daily ELT pipeline: S3 CSV → Parquet warehouse → Athena mart. "
            "Runs D-1 data every morning at 06:00 UTC."
        ),
        tags=["adstart", "daily", "production"],
        # Retry config (Prefect v2)
        flow_runner_kwargs={},
    )
    return deployment


def create_backfill_deployment() -> Deployment:
    """
    Deployment riêng cho backfill — trigger thủ công khi cần.
    Không có schedule — chạy on-demand với explicit run_date + backfill_days.
    """
    deployment = Deployment.build_from_flow(
        flow=run_pipeline,
        name="backfill",
        version="1.0.0",
        work_queue_name="default",
        schedule=None,                         # No automatic schedule
        parameters={
            "run_date":      None,
        },
        description="On-demand backfill deployment. Set run_date + backfill_days as params.",
        tags=["adstart", "backfill"],
    )
    return deployment


if __name__ == "__main__":
    print("Deploying adstart pipeline to Prefect...")

    daily = create_deployment()
    daily_id = daily.apply()
    print(f"  ✓ Daily deployment ID : {daily_id}")

    backfill = create_backfill_deployment()
    backfill_id = backfill.apply()
    print(f"  ✓ Backfill deployment : {backfill_id}")

    print("\nDeployments created ✓")
    print("\nNext commands:")
    print("  # Start Prefect UI (nếu chưa chạy)")
    print("  prefect server start")
    print("")
    print("  # Trigger manual run cho ngày cụ thể")
    print("  prefect deployment run 'adstart-daily-pipeline/daily' \\")
    print("    --param run_date=2026-01-15")
    print("")
    print("  # View deployments")
    print("  prefect deployment ls")
    print("")
    print("  # Open UI")
    print("  open http://127.0.0.1:4200")
