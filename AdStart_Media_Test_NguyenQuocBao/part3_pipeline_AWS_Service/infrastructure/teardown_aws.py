"""
infrastructure/teardown_aws.py — Xóa toàn bộ AWS resources sau khi dùng xong.

⚠️  CẢNH BÁO: Script này XÓA TẤT CẢ data và resources.
     Chỉ dùng cho môi trường dev/test để tránh phí AWS.

Usage:
    python infrastructure/teardown_aws.py --account-id 123456789012 --confirm
"""
from __future__ import annotations

import argparse
import logging
import sys

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def empty_and_delete_bucket(s3, bucket_name: str) -> None:
    """Xóa tất cả objects (kể cả versions) rồi delete bucket."""
    try:
        # Delete tất cả object versions (versioned bucket)
        paginator = s3.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket_name):
            versions = page.get("Versions", []) + page.get("DeleteMarkers", [])
            if versions:
                s3.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": [{"Key": v["Key"], "VersionId": v["VersionId"]}
                                        for v in versions]},
                )
        s3.delete_bucket(Bucket=bucket_name)
        logger.info(f"  ✓ Deleted s3://{bucket_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            logger.info(f"  ↩ Bucket không tồn tại: {bucket_name}")
        else:
            logger.error(f"  ✗ Error deleting {bucket_name}: {e}")


def delete_glue_database(glue, db_name: str) -> None:
    try:
        # Delete all tables first
        tables = glue.get_tables(DatabaseName=db_name)["TableList"]
        for t in tables:
            glue.delete_table(DatabaseName=db_name, Name=t["Name"])
        glue.delete_database(Name=db_name)
        logger.info(f"  ✓ Deleted Glue database: {db_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            logger.info(f"  ↩ Glue DB không tồn tại: {db_name}")
        else:
            logger.error(f"  ✗ Error: {e}")


def delete_iam_role(iam, role_name: str) -> None:
    try:
        # Remove inline policies first
        policies = iam.list_role_policies(RoleName=role_name)["PolicyNames"]
        for p in policies:
            iam.delete_role_policy(RoleName=role_name, PolicyName=p)
        # Detach managed policies
        for p in iam.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=p["PolicyArn"])
        iam.delete_role(RoleName=role_name)
        logger.info(f"  ✓ Deleted IAM role: {role_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            logger.info(f"  ↩ IAM role không tồn tại: {role_name}")
        else:
            logger.error(f"  ✗ Error: {e}")


def delete_sns_topic(sns, topic_name: str, region: str, account_id: str) -> None:
    arn = f"arn:aws:sns:{region}:{account_id}:{topic_name}"
    try:
        sns.delete_topic(TopicArn=arn)
        logger.info(f"  ✓ Deleted SNS topic: {arn}")
    except ClientError as e:
        logger.info(f"  ↩ SNS topic: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--confirm", action="store_true",
                        help="Bắt buộc phải có flag này để thực sự xóa")
    args = parser.parse_args()

    if not args.confirm:
        print("⚠️  Thêm --confirm để thực sự xóa resources.")
        print("   Tất cả S3 data, Glue tables, IAM role sẽ bị xóa vĩnh viễn!")
        sys.exit(1)

    session = boto3.Session(region_name=args.region)
    s3   = session.client("s3")
    glue = session.client("glue")
    iam  = session.client("iam")
    sns  = session.client("sns")

    print(f"\n⚠️  TEARDOWN — Account: {args.account_id} | Region: {args.region}\n")

    print("[ S3 ] Deleting buckets ...")
    for bucket in [
        f"adstart-raw-{args.account_id}",
        f"adstart-warehouse-{args.account_id}",
        f"adstart-athena-results-{args.account_id}",
    ]:
        empty_and_delete_bucket(s3, bucket)

    print("\n[ Glue ] Deleting databases ...")
    delete_glue_database(glue, "adstart_raw")
    delete_glue_database(glue, "adstart_warehouse")

    print("\n[ IAM ] Deleting role ...")
    delete_iam_role(iam, "adstart-pipeline-role")

    print("\n[ SNS ] Deleting topic ...")
    delete_sns_topic(sns, "adstart-pipeline-alerts", args.region, args.account_id)

    print("\nTeardown COMPLETE ✓ — Tất cả resources đã bị xóa.")
