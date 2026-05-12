"""
infrastructure/upload_sample_data.py — Upload local CSV files lên S3.

Dùng khi:
  - Lần đầu setup môi trường AWS (sau setup_aws.py)
  - Test pipeline với sample data
  - Simulate operator file delivery

Usage:
    python infrastructure/upload_sample_data.py --date 2026-01-15
    python infrastructure/upload_sample_data.py --date 2026-01-15 --operator operator_a
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def upload_operator_files(s3_client, raw_bucket: str, run_date: date, operators: list[str]) -> None:
    """Upload operator CSV files vào S3 với Hive partition layout."""
    data_dir = Path("data/raw")

    operator_map = {
        "operator_a": "operator_A.csv",
        "operator_b": "operator_B.csv",
        "operator_c": "operator_C.csv",
    }

    for op_key in operators:
        filename = operator_map.get(op_key)
        if not filename:
            logger.warning(f"Unknown operator: {op_key}")
            continue

        local_path = data_dir / filename
        if not local_path.exists():
            logger.error(f"File không tìm thấy: {local_path}")
            continue

        # S3 key với Hive partition layout
        s3_key = f"{op_key}/date={run_date}/data.csv"
        s3_client.upload_file(str(local_path), raw_bucket, s3_key)
        logger.info(f"  ✓ Uploaded {filename} → s3://{raw_bucket}/{s3_key}")


def upload_static_files(s3_client, raw_bucket: str) -> None:
    """Upload static reference files."""
    data_dir = Path("data/raw")
    static_files = ["campaigns.csv", "clicks.csv", "tracking_codes.csv", "page_events.csv"]

    for filename in static_files:
        local_path = data_dir / filename
        if not local_path.exists():
            logger.warning(f"Static file không tìm thấy: {local_path} — bỏ qua")
            continue

        s3_key = f"static/{filename}"
        s3_client.upload_file(str(local_path), raw_bucket, s3_key)
        logger.info(f"  ✓ Uploaded {filename} → s3://{raw_bucket}/{s3_key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload sample data lên S3")
    parser.add_argument("--date", required=True, type=lambda s: date.fromisoformat(s))
    parser.add_argument("--operator", default="all",
                        help="operator_a | operator_b | operator_c | all")
    args = parser.parse_args()

    from dotenv import load_dotenv
    import os, boto3
    load_dotenv()

    raw_bucket = os.getenv("AWS_RAW_BUCKET")
    if not raw_bucket:
        logger.error("AWS_RAW_BUCKET not set. Chạy setup_aws.py trước.")
        sys.exit(1)

    region = os.getenv("AWS_REGION", "eu-west-1")
    s3     = boto3.client("s3", region_name=region)

    ops = (
        ["operator_a", "operator_b", "operator_c"]
        if args.operator == "all"
        else [args.operator]
    )

    print(f"\nUploading to s3://{raw_bucket}/ for date={args.date}")
    upload_operator_files(s3, raw_bucket, args.date, ops)
    upload_static_files(s3, raw_bucket)
    print("\nUpload complete ✓")
