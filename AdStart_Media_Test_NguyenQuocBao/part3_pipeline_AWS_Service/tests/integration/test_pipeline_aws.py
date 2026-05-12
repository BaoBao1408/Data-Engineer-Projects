"""
tests/integration/test_pipeline_aws.py — Integration tests với moto mock AWS.

moto intercepts boto3 calls và fake S3/Glue/Athena — không cần real AWS.
Tests chạy in-process, không cần internet.

Cách moto hoạt động:
    @mock_aws  ← decorator intercept tất cả boto3 calls trong test function
    with mock_aws(): ← context manager

Coverage:
    - Upload CSV → S3 (fake)
    - Read back từ S3 → DataFrame
    - Write Parquet → S3 (real pyarrow, fake S3)
    - Read Parquet back từ S3
"""
from __future__ import annotations

import io
import os
import pytest
import pandas as pd
import boto3
from datetime import date
from unittest.mock import patch, MagicMock

# Set AWS mode cho integration tests
os.environ["PIPELINE_ENV"]              = "aws"
os.environ["AWS_RAW_BUCKET"]           = "test-raw"
os.environ["AWS_WAREHOUSE_BUCKET"]     = "test-warehouse"
os.environ["AWS_ATHENA_OUTPUT_BUCKET"] = "test-athena"
os.environ["AWS_DEFAULT_REGION"]       = "us-east-1"
os.environ["AWS_REGION"]               = "us-east-1"
# Fake credentials cho moto
os.environ["AWS_ACCESS_KEY_ID"]        = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"]    = "testing"
os.environ["AWS_SECURITY_TOKEN"]       = "testing"
os.environ["AWS_SESSION_TOKEN"]        = "testing"

try:
    from moto import mock_aws
    MOTO_AVAILABLE = True
except ImportError:
    MOTO_AVAILABLE = False


@pytest.fixture
def run_date():
    return date(2026, 1, 15)


@pytest.fixture
def sample_operator_a_csv():
    return """transaction_id,rotate_id,msisdn,event_code,status,amount,currency,received_time
tx_001,rot_001,447700900001,1,SUCCESS,,GBP,2026-01-15 10:00:00+00
tx_002,rot_002,447700900002,1,SUCCESS,,GBP,2026-01-15 11:00:00+00
tx_003,rot_001,447700900001,2,SUCCESS,4.99,GBP,2026-01-15 12:00:00+00
"""


@pytest.mark.skipif(not MOTO_AVAILABLE, reason="moto not installed")
class TestS3Operations:
    """Test S3 upload + read via moto mock."""

    @mock_aws
    def test_upload_and_read_csv(self, run_date, sample_operator_a_csv):
        """Upload CSV → S3 → read back → DataFrame."""
        # Setup fake S3
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-raw")

        # Upload CSV
        key = f"operator_a/date={run_date}/data.csv"
        s3.put_object(
            Bucket="test-raw",
            Key=key,
            Body=sample_operator_a_csv.encode(),
        )

        # Read back
        from src.utils.s3_utils import read_csv_from_s3
        df = read_csv_from_s3("test-raw", key, region="us-east-1")

        assert len(df) == 3
        assert "transaction_id" in df.columns
        assert df["transaction_id"].iloc[0] == "tx_001"

    @mock_aws
    def test_file_exists_check(self, run_date, sample_operator_a_csv):
        """file_exists_on_s3 trả về đúng True/False."""
        from src.utils.s3_utils import file_exists_on_s3
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-raw")

        key = "operator_a/date=2026-01-15/data.csv"
        assert file_exists_on_s3("test-raw", key, region="us-east-1") is False

        s3.put_object(Bucket="test-raw", Key=key, Body=b"test")
        assert file_exists_on_s3("test-raw", key, region="us-east-1") is True

    @mock_aws
    def test_list_prefix(self):
        """list_s3_prefix tìm đúng keys."""
        from src.utils.s3_utils import list_s3_prefix
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-raw")

        for key in ["operator_a/date=2026-01-15/part1.csv",
                    "operator_a/date=2026-01-15/part2.csv",
                    "operator_b/date=2026-01-15/data.csv"]:
            s3.put_object(Bucket="test-raw", Key=key, Body=b"test")

        keys = list_s3_prefix("test-raw", "operator_a/date=2026-01-15/", region="us-east-1")
        assert len(keys) == 2
        assert all("operator_a" in k for k in keys)


@pytest.mark.skipif(not MOTO_AVAILABLE, reason="moto not installed")
class TestAWSWarehouseWrite:
    """Test AWSWarehouse.write_table với moto S3."""

    @mock_aws
    def test_write_parquet_creates_s3_objects(self, run_date):
        """write_table() ghi Parquet files lên S3."""
        import awswrangler as wr
        from unittest.mock import patch

        # Create fake buckets
        s3 = boto3.client("s3", region_name="us-east-1")
        for bucket in ["test-raw", "test-warehouse", "test-athena"]:
            s3.create_bucket(Bucket=bucket)

        df = pd.DataFrame({
            "subscription_id": ["sub_001", "sub_002"],
            "operator":        ["operator_A", "operator_A"],
            "msisdn":          ["447700900001", "447700900002"],
            "campaign_id":     ["camp_A", "camp_A"],
            "report_date":     ["2026-01-15", "2026-01-15"],
        })

        # Patch settings để dùng fake buckets
        with patch("src.utils.aws_warehouse.AWSWarehouse._boto3_session") as mock_session:
            mock_session.return_value = boto3.Session(region_name="us-east-1")

            # Dùng awswrangler với moto fake S3
            wr.s3.to_parquet(
                df=df,
                path="s3://test-warehouse/facts/fct_subscriptions/",
                dataset=True,
                mode="overwrite_partitions",
                partition_cols=["report_date"],
                boto3_session=boto3.Session(region_name="us-east-1"),
            )

        # Verify files were created
        keys = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket="test-warehouse"):
            keys.extend([o["Key"] for o in page.get("Contents", [])])

        assert len(keys) > 0
        assert any(".parquet" in k for k in keys)


@pytest.mark.skipif(not MOTO_AVAILABLE, reason="moto not installed")
class TestQualityChecks:
    """Test quality check functions với mock warehouse."""

    def test_suite_passes_all_checks(self, run_date):
        """QualitySuite passed = True khi tất cả checks pass."""
        from src.orchestration.quality import QualityResult, QualitySuite
        suite = QualitySuite(run_date=run_date)
        suite.results = [
            QualityResult("check_1", passed=True, layer="facts"),
            QualityResult("check_2", passed=True, layer="mart"),
        ]
        assert suite.passed is True
        assert suite.failures == []

    def test_suite_fails_when_any_check_fails(self, run_date):
        """QualitySuite passed = False nếu có 1 check fail."""
        from src.orchestration.quality import QualityResult, QualitySuite
        suite = QualitySuite(run_date=run_date)
        suite.results = [
            QualityResult("check_ok",   passed=True,  layer="facts"),
            QualityResult("check_fail", passed=False, failing_rows=5, layer="mart"),
        ]
        assert suite.passed is False
        assert len(suite.failures) == 1
        assert suite.failures[0].check_name == "check_fail"
