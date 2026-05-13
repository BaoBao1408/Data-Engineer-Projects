"""
src/utils/s3_utils.py — S3 helper functions.

Used for:
  - Uploading local CSV files to S3 (testing / initial bootstrap)
  - Checking whether a file exists on S3 (retry logic support)
  - Listing files inside an S3 prefix
  - Downloading S3 files into pandas DataFrames

Why keep this module separate?
  - Keeps loaders.py clean and focused on business logic
  - S3 operations can be reused across multiple pipelines
  - Easier to mock during unit testing
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def get_s3_client(region: str = "eu-west-1"):
    """Return a boto3 S3 client."""
    import boto3
    return boto3.client("s3", region_name=region)


def get_s3_resource(region: str = "eu-west-1"):
    """Return a boto3 S3 resource."""
    import boto3
    return boto3.resource("s3", region_name=region)


def file_exists_on_s3(bucket: str, key: str, region: str = "eu-west-1") -> bool:
    """
    Check whether a file exists on S3.
    Commonly used in retry logic when waiting for upstream uploads.

    Equivalent to:
        os.path.exists("data/raw/operator_A.csv")
    """
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3", region_name=region)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def list_s3_prefix(bucket: str, prefix: str, region: str = "eu-west-1") -> list[str]:
    """
    List all object keys inside an S3 prefix.
    Useful for retrieving all files for a specific date or operator.

    Example:
        keys = list_s3_prefix("adstart-raw", "operator_a/date=2026-01-15/")
        # -> ["operator_a/date=2026-01-15/data.csv"]
    """
    import boto3
    s3 = boto3.client("s3", region_name=region)
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_csv_from_s3(
    bucket: str,
    key: str,
    region: str = "eu-west-1",
    **read_csv_kwargs,
) -> pd.DataFrame:
    """
    Read a CSV file from S3 into a pandas DataFrame.

    Replaces:
        pd.read_csv("data/raw/operator_A.csv")

    With:
        read_csv_from_s3("adstart-raw", "operator_a/date=2026-01-15/data.csv")
    """
    import boto3
    import io
    s3 = boto3.client("s3", region_name=region)
    logger.debug(f"Reading s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()), **read_csv_kwargs)


def upload_csv_to_s3(
    local_path: Path,
    bucket: str,
    key: str,
    region: str = "eu-west-1",
) -> str:
    """
    Upload a local CSV file to S3.
    Used in infrastructure/upload_sample_data.py
    for test environment setup.

    Returns the full S3 URI.
    """
    import boto3
    s3 = boto3.client("s3", region_name=region)
    s3.upload_file(str(local_path), bucket, key)
    s3_uri = f"s3://{bucket}/{key}"
    logger.info(f"Uploaded {local_path.name} -> {s3_uri}")
    return s3_uri


def read_parquet_from_s3(
    path: str,
    region: str = "eu-west-1",
    filters: list | None = None,
) -> pd.DataFrame:
    """
    Read Parquet files from S3 (supports partitioned datasets).

    path:
        s3://bucket/prefix/
        Can point to a folder containing multiple parquet files.

    filters:
        PyArrow partition filters.
        Example:
            [("report_date", "=", "2026-01-15")]

    Uses awswrangler if available,
    otherwise falls back to pyarrow + s3fs.
    """
    try:
        import awswrangler as wr
        import boto3
        session = boto3.Session(region_name=region)
        return wr.s3.read_parquet(
            path=path,
            dataset=True,
            filters=filters,
            boto3_session=session,
        )
    except ImportError:
        import pyarrow.parquet as pq
        import s3fs
        fs = s3fs.S3FileSystem(region_name=region)

        # Remove the s3:// prefix for s3fs compatibility
        clean_path = path.replace("s3://", "")

        dataset = pq.ParquetDataset(
            clean_path,
            filesystem=fs,
            filters=filters,
        )
        return dataset.read_pandas().to_pandas()


def write_parquet_to_s3(
    df: pd.DataFrame,
    path: str,
    partition_cols: list[str] | None = None,
    mode: str = "overwrite_partitions",
    glue_database: str | None = None,
    glue_table: str | None = None,
    region: str = "eu-west-1",
) -> None:
    """
    Write a DataFrame to S3 in Parquet format.

    mode="overwrite_partitions" provides idempotent behaviour:
        - Removes the old partition
        - Writes the new partition
        - Safe for rerunning pipelines for the same date

    If glue_database + glue_table are provided:
        - Automatically creates/updates Glue Catalog tables
        - Athena can query the data immediately after writing

    Equivalent DuckDB pattern:
        DELETE FROM fct_subscriptions
        WHERE report_date = '2026-01-15';

        INSERT INTO fct_subscriptions
        SELECT ...;
    """
    import awswrangler as wr
    import boto3
    session = boto3.Session(region_name=region)

    kwargs = {
        "df": df,
        "path": path,
        "dataset": True,
        "mode": mode,
        "boto3_session": session,
    }

    if partition_cols:
        kwargs["partition_cols"] = partition_cols

    if glue_database and glue_table:
        kwargs["database"] = glue_database
        kwargs["table"] = glue_table

    wr.s3.to_parquet(**kwargs)

    logger.info(
        f"Written {len(df):,} rows -> {path} "
        f"(mode={mode}, partitions={partition_cols})"
    )


def delete_s3_partition(
    bucket: str,
    prefix: str,
    run_date: date,
    region: str = "eu-west-1",
) -> int:
    """
    Delete all objects inside a specific date partition.
    Commonly used for:
        - Manual cleanup
        - Reprocessing partitions
        - Partition overwrite preparation

    prefix example:
        "facts/fct_subscriptions/report_date=2026-01-15/"
    """
    import boto3
    s3 = boto3.client("s3", region_name=region)

    full_prefix = f"{prefix}/report_date={run_date}/"

    keys = list_s3_prefix(bucket, full_prefix, region)

    if not keys:
        return 0

    objects = [{"Key": k} for k in keys]

    s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": objects},
    )

    logger.info(
        f"Deleted {len(objects)} objects from "
        f"s3://{bucket}/{full_prefix}"
    )

    return len(objects)