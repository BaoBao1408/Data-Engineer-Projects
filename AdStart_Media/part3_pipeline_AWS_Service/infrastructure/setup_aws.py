"""
infrastructure/setup_aws.py — Tạo toàn bộ AWS resources cần thiết.

Chạy 1 lần trước khi deploy pipeline lần đầu.
Script này idempotent — chạy lại không tạo duplicate.

Resources được tạo:
  S3 Buckets (3):
    - adstart-raw-<account_id>          : raw CSV files từ operators
    - adstart-warehouse-<account_id>    : Parquet warehouse layers
    - adstart-athena-results-<account_id>: Athena query results

  S3 Bucket settings:
    - Versioning enabled (raw bucket)
    - Server-side encryption (AES-256)
    - Block public access (tất cả)
    - Lifecycle policy: tự động xóa Athena results sau 7 ngày

  Glue Catalog Databases (2):
    - adstart_raw       : raw Parquet tables
    - adstart_warehouse : facts + mart tables

  IAM Role (1):
    - adstart-pipeline-role : cho EC2/ECS/Lambda chạy pipeline

  SNS Topic (1):
    - adstart-pipeline-alerts : nhận quality check failures

Usage:
    python infrastructure/setup_aws.py --account-id 123456789012 --region eu-west-1
    python infrastructure/setup_aws.py --account-id 123456789012 --region eu-west-1 --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

DRY_RUN = False  # Set by --dry-run flag


# ── S3 ────────────────────────────────────────────────────────────

def create_s3_bucket(s3, bucket_name: str, region: str) -> bool:
    """Tạo S3 bucket với encryption + block public access. Idempotent."""
    if DRY_RUN:
        logger.info(f"[DRY-RUN] Would create s3://{bucket_name}")
        return True

    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        logger.info(f"  ✓ Created s3://{bucket_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            logger.info(f"  ↩ Bucket already exists: s3://{bucket_name}")
        else:
            logger.error(f"  ✗ Failed to create {bucket_name}: {e}")
            return False

    # Block all public access
    s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls":       True,
            "IgnorePublicAcls":      True,
            "BlockPublicPolicy":     True,
            "RestrictPublicBuckets": True,
        },
    )

    # Server-side encryption (AES-256, no extra cost)
    s3.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "AES256"
                },
                "BucketKeyEnabled": True,
            }]
        },
    )
    logger.info(f"  ✓ Encryption + public-block configured for {bucket_name}")
    return True


def enable_versioning(s3, bucket_name: str) -> None:
    """Enable versioning trên raw bucket (audit trail cho raw files)."""
    if DRY_RUN:
        logger.info(f"[DRY-RUN] Would enable versioning on {bucket_name}")
        return
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )
    logger.info(f"  ✓ Versioning enabled on {bucket_name}")


def add_lifecycle_policy(s3, bucket_name: str, prefix: str, expire_days: int) -> None:
    """
    Lifecycle rule: xóa objects sau expire_days ngày.
    Dùng cho Athena results bucket để tránh tốn phí storage.
    """
    if DRY_RUN:
        logger.info(f"[DRY-RUN] Would add lifecycle {expire_days}d on {bucket_name}/{prefix}")
        return
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            "Rules": [{
                "ID":     f"expire-{prefix.strip('/')}-after-{expire_days}d",
                "Status": "Enabled",
                "Filter": {"Prefix": prefix},
                "Expiration": {"Days": expire_days},
            }]
        },
    )
    logger.info(f"  ✓ Lifecycle policy: {bucket_name}/{prefix} → expire after {expire_days} days")


def create_s3_folder_structure(s3, bucket_name: str) -> None:
    """Tạo folder prefixes trong warehouse bucket."""
    if DRY_RUN:
        logger.info(f"[DRY-RUN] Would create folder structure in {bucket_name}")
        return
    for prefix in ["raw/", "dimensions/", "facts/", "mart/"]:
        s3.put_object(Bucket=bucket_name, Key=prefix)
    logger.info(f"  ✓ Folder structure created in {bucket_name}")


# ── Glue ─────────────────────────────────────────────────────────

def create_glue_database(glue, db_name: str, description: str) -> None:
    """Tạo Glue Catalog database. Idempotent."""
    if DRY_RUN:
        logger.info(f"[DRY-RUN] Would create Glue database: {db_name}")
        return
    try:
        glue.create_database(
            DatabaseInput={
                "Name":        db_name,
                "Description": description,
            }
        )
        logger.info(f"  ✓ Glue database created: {db_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            logger.info(f"  ↩ Glue database already exists: {db_name}")
        else:
            raise


# ── IAM ──────────────────────────────────────────────────────────

PIPELINE_ROLE_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect":    "Allow",
        "Principal": {"Service": ["ec2.amazonaws.com", "ecs-tasks.amazonaws.com",
                                  "lambda.amazonaws.com"]},
        "Action":    "sts:AssumeRole",
    }]
}

PIPELINE_INLINE_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        # S3: Read raw files + Read/Write warehouse + Read/Write Athena results
        {
            "Sid":    "S3PipelineAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                "s3:ListBucket", "s3:GetBucketLocation",
                "s3:ListBucketMultipartUploads", "s3:AbortMultipartUpload",
            ],
            "Resource": [
                "arn:aws:s3:::adstart-raw-*",
                "arn:aws:s3:::adstart-raw-*/*",
                "arn:aws:s3:::adstart-warehouse-*",
                "arn:aws:s3:::adstart-warehouse-*/*",
                "arn:aws:s3:::adstart-athena-results-*",
                "arn:aws:s3:::adstart-athena-results-*/*",
            ],
        },
        # Glue: Read/Write catalog tables + databases
        {
            "Sid":    "GlueCatalogAccess",
            "Effect": "Allow",
            "Action": [
                "glue:CreateDatabase", "glue:GetDatabase", "glue:GetDatabases",
                "glue:CreateTable", "glue:UpdateTable", "glue:GetTable",
                "glue:GetTables", "glue:DeleteTable",
                "glue:CreatePartition", "glue:BatchCreatePartition",
                "glue:GetPartition", "glue:GetPartitions",
                "glue:UpdatePartition", "glue:BatchDeletePartition",
            ],
            "Resource": [
                "arn:aws:glue:*:*:catalog",
                "arn:aws:glue:*:*:database/adstart_*",
                "arn:aws:glue:*:*:table/adstart_*/*",
            ],
        },
        # Athena: Start + manage query executions
        {
            "Sid":    "AthenaAccess",
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution", "athena:GetQueryExecution",
                "athena:GetQueryResults", "athena:StopQueryExecution",
                "athena:ListQueryExecutions", "athena:GetWorkGroup",
            ],
            "Resource": "*",
        },
        # SNS: Publish alerts
        {
            "Sid":      "SNSPublish",
            "Effect":   "Allow",
            "Action":   ["sns:Publish"],
            "Resource": "arn:aws:sns:*:*:adstart-*",
        },
        # CloudWatch Logs: cho Lambda/ECS logging
        {
            "Sid":    "CloudWatchLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:log-group:/aws/adstart-pipeline/*",
        },
    ]
}


def create_iam_role(iam, role_name: str, account_id: str) -> Optional[str]:
    """
    Tạo IAM role cho pipeline.
    Returns ARN của role.
    """
    if DRY_RUN:
        logger.info(f"[DRY-RUN] Would create IAM role: {role_name}")
        return f"arn:aws:iam::{account_id}:role/{role_name}"

    try:
        resp = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(PIPELINE_ROLE_TRUST_POLICY),
            Description="IAM role cho adstart data pipeline (S3, Glue, Athena, SNS)",
            Tags=[{"Key": "Project", "Value": "adstart-pipeline"}],
        )
        role_arn = resp["Role"]["Arn"]
        logger.info(f"  ✓ IAM role created: {role_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
            logger.info(f"  ↩ IAM role already exists: {role_name}")
        else:
            raise

    # Attach inline policy
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="adstart-pipeline-policy",
        PolicyDocument=json.dumps(PIPELINE_INLINE_POLICY),
    )
    logger.info(f"  ✓ Inline policy attached to {role_name}")

    return role_arn


# ── SNS ───────────────────────────────────────────────────────────

def create_sns_topic(sns, topic_name: str) -> str:
    """Tạo SNS topic cho pipeline alerts. Returns ARN."""
    if DRY_RUN:
        logger.info(f"[DRY-RUN] Would create SNS topic: {topic_name}")
        return f"arn:aws:sns:us-east-1:000000000000:{topic_name}"

    resp = sns.create_topic(Name=topic_name)
    arn  = resp["TopicArn"]
    logger.info(f"  ✓ SNS topic: {arn}")
    return arn


def subscribe_email_to_sns(sns, topic_arn: str, email: str) -> None:
    """Subscribe email tới SNS topic."""
    if DRY_RUN:
        logger.info(f"[DRY-RUN] Would subscribe {email} → {topic_arn}")
        return
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    logger.info(f"  ✓ Email subscription pending confirmation: {email}")


# ── .env file generator ───────────────────────────────────────────

def write_env_file(account_id: str, region: str, sns_topic_arn: str) -> None:
    """Tạo file .env với đúng bucket names để dùng trong config/base.py."""
    env_content = f"""# Auto-generated by infrastructure/setup_aws.py
# Copy file này thành .env và đừng commit lên git

PIPELINE_ENV=aws

AWS_REGION={region}
AWS_RAW_BUCKET=adstart-raw-{account_id}
AWS_WAREHOUSE_BUCKET=adstart-warehouse-{account_id}
AWS_ATHENA_OUTPUT_BUCKET=adstart-athena-results-{account_id}

GLUE_RAW_DATABASE=adstart_raw
GLUE_WAREHOUSE_DATABASE=adstart_warehouse

SNS_ALERT_TOPIC_ARN={sns_topic_arn}
"""
    with open(".env", "w") as f:
        f.write(env_content)
    logger.info("  ✓ .env file written (add to .gitignore!)")


# ── Main ──────────────────────────────────────────────────────────

def setup(account_id: str, region: str, alert_email: Optional[str] = None) -> None:
    session  = boto3.Session(region_name=region)
    s3       = session.client("s3")
    glue     = session.client("glue")
    iam      = session.client("iam")
    sns      = session.client("sns")

    raw_bucket       = f"adstart-raw-{account_id}"
    warehouse_bucket = f"adstart-warehouse-{account_id}"
    athena_bucket    = f"adstart-athena-results-{account_id}"

    print("\n══════════════════════════════════════════")
    print("  adstart Pipeline — AWS Setup")
    print(f"  Account: {account_id} | Region: {region}")
    if DRY_RUN:
        print("  MODE: DRY-RUN (không tạo resources thật)")
    print("══════════════════════════════════════════\n")

    # ── S3 Buckets ────────────────────────────────────────────────
    print("[ S3 ] Creating buckets ...")
    create_s3_bucket(s3, raw_bucket, region)
    enable_versioning(s3, raw_bucket)                   # Audit trail cho raw files

    create_s3_bucket(s3, warehouse_bucket, region)
    create_s3_folder_structure(s3, warehouse_bucket)

    create_s3_bucket(s3, athena_bucket, region)
    add_lifecycle_policy(s3, athena_bucket, "query-results/", expire_days=7)

    # ── Glue Databases ────────────────────────────────────────────
    print("\n[ Glue ] Creating catalog databases ...")
    create_glue_database(glue, "adstart_raw",
                         "Raw Parquet tables loaded từ operator CSV files")
    create_glue_database(glue, "adstart_warehouse",
                         "Fact tables + mart tables (Athena queryable)")

    # ── IAM Role ──────────────────────────────────────────────────
    print("\n[ IAM ] Creating pipeline role ...")
    role_arn = create_iam_role(iam, "adstart-pipeline-role", account_id)

    # ── SNS Topic ─────────────────────────────────────────────────
    print("\n[ SNS ] Creating alert topic ...")
    sns_arn = create_sns_topic(sns, "adstart-pipeline-alerts")
    if alert_email:
        subscribe_email_to_sns(sns, sns_arn, alert_email)

    # ── Write .env ────────────────────────────────────────────────
    print("\n[ Config ] Writing .env file ...")
    write_env_file(account_id, region, sns_arn)

    print("\n══════════════════════════════════════════")
    print("  Setup COMPLETE ✓")
    print(f"\n  Raw bucket     : s3://{raw_bucket}/")
    print(f"  Warehouse      : s3://{warehouse_bucket}/")
    print(f"  Athena results : s3://{athena_bucket}/")
    print(f"  IAM role ARN   : {role_arn}")
    print(f"  SNS topic      : {sns_arn}")
    print("\n  Next steps:")
    print("  1. Upload sample data:")
    print("     python infrastructure/upload_sample_data.py")
    print("  2. Run pipeline:")
    print("     python -m src.orchestration.pipeline --date 2026-01-15")
    print("══════════════════════════════════════════\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup AWS resources cho adstart pipeline")
    parser.add_argument("--account-id", required=True, help="AWS Account ID (12 digits)")
    parser.add_argument("--region",     default="eu-west-1", help="AWS region (default: eu-west-1)")
    parser.add_argument("--alert-email", default=None, help="Email để nhận SNS alerts")
    parser.add_argument("--dry-run", action="store_true", help="Preview mà không tạo resources thật")
    args = parser.parse_args()

    if args.dry_run:
        DRY_RUN = True

    setup(
        account_id=args.account_id,
        region=args.region,
        alert_email=args.alert_email,
    )
