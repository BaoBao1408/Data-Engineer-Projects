"""
src/ingest/loaders.py — Load raw source files into the warehouse staging layer.

LOCAL mode : reads CSV from data/raw/ -> pandas DataFrame -> DuckDB
AWS mode   : reads CSV from S3 -> pandas DataFrame -> S3 Parquet + Glue Catalog

Each loader follows the same five-step contract:
  1. Locate the source file (S3 prefix or local path)
  2. Read CSV into a pandas DataFrame
  3. Rename and cast columns to match the target schema
  4. Validate quality (null rate + minimum row count)
  5. Write to the warehouse (DuckDB locally, S3 Parquet on AWS)
  6. Return a result dict for the pipeline to log

AWS S3 raw bucket layout:
  s3://adstart-raw-<account_id>/
  ├── operator_a/date=2026-01-15/data.csv
  ├── operator_b/date=2026-01-15/data.csv
  ├── operator_c/date=2026-01-15/data.csv
  └── static/
      ├── campaigns.csv
      ├── clicks.csv
      ├── tracking_codes.csv
      └── page_events.csv
"""
from __future__ import annotations

import io
import logging
from datetime import date
from pathlib import Path

import pandas as pd

from config.base import settings
from src.ingest.validator import validate_dataframe, REQUIRED_COLUMNS
from src.utils.aws_warehouse import AWSWarehouse

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal read helpers
# ---------------------------------------------------------------------------

def _read_csv_local(filename: str) -> pd.DataFrame:
    """Read a CSV file from the local data/raw/ directory."""
    path = settings.data_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")
    return pd.read_csv(path, low_memory=False)


def _read_csv_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a single CSV file from S3 into a DataFrame."""
    import boto3
    s3 = boto3.client("s3", region_name=settings.aws_region)
    logger.debug("Reading s3://%s/%s", bucket, key)
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()), low_memory=False)


def _read_all_csvs_in_prefix(bucket: str, prefix: str) -> pd.DataFrame:
    """
    Read and concatenate every CSV file found under an S3 prefix.

    Operators occasionally split a single day's delivery across multiple
    files — this handles that transparently.
    """
    import boto3
    s3 = boto3.client("s3", region_name=settings.aws_region)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".csv")]
    if not keys:
        raise FileNotFoundError(f"No CSV files found at s3://{bucket}/{prefix}")
    frames = [_read_csv_s3(bucket, k) for k in keys]
    return pd.concat(frames, ignore_index=True)


def _get_operator_df(operator_key: str, run_date: date) -> pd.DataFrame:
    """
    Route an operator read to the correct source depending on the environment.

    LOCAL : reads from data/raw/<filename>
    AWS   : reads from s3://raw-bucket/<operator>/date=YYYY-MM-DD/
    """
    if settings.is_aws:
        prefix = f"{settings.operator_s3_prefixes[operator_key]}/date={run_date}/"
        return _read_all_csvs_in_prefix(settings.raw_bucket, prefix)
    filename = settings.operator_files[operator_key]
    return _read_csv_local(filename)


def _get_static_df(file_key: str) -> pd.DataFrame:
    """
    Route a static reference file read to the correct source.

    LOCAL : reads from data/raw/<filename>
    AWS   : reads from s3://raw-bucket/static/<filename>
    """
    if settings.is_aws:
        key = f"{settings.static_s3_prefix}/{settings.static_files[file_key]}"
        return _read_csv_s3(settings.raw_bucket, key)
    return _read_csv_local(settings.static_files[file_key])


# ---------------------------------------------------------------------------
# Operator A
# ---------------------------------------------------------------------------

def load_operator_a(warehouse: AWSWarehouse, run_date: date) -> dict:
    """
    Load operator_A source file into the raw_operator_a staging table.

    Source columns : transaction_id, rotate_id, msisdn, event_code,
                     status, amount, currency, received_time
    Staged columns : same, with received_time renamed to event_time
                     and _loaded_date added as a partition marker.

    event_code semantics:
        1 = subscribe
        2 = billing charge
        3 = unsubscribe
    """
    df = _get_operator_df("operator_a", run_date)

    # received_time is renamed to event_time to avoid a reserved-keyword
    # collision in both DuckDB and Athena.
    df = df.rename(columns={"received_time": "event_time"})

    df["event_code"]   = pd.to_numeric(df["event_code"], errors="coerce").astype("Int64")
    df["amount"]       = pd.to_numeric(df["amount"], errors="coerce")
    df["event_time"]   = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
    df["_loaded_date"] = str(run_date)

    for col in ["transaction_id", "rotate_id", "msisdn", "status", "currency"]:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    result = validate_dataframe(df, "raw_operator_a", REQUIRED_COLUMNS["raw_operator_a"])
    warehouse.write_table(
        df, layer="raw", table="raw_operator_a",
        partition_date=run_date, mode="overwrite_partitions",
    )
    return result


# ---------------------------------------------------------------------------
# Operator B
# ---------------------------------------------------------------------------

def load_operator_b(warehouse: AWSWarehouse, run_date: date) -> dict:
    """
    Load operator_B source file into the raw_operator_b staging table.

    Source columns : transaction_id, rotate_id, msisdn, transaction_type,
                     amount, currency, received_time

    Design note: REN and UNSUB rows intentionally carry a NULL rotate_id.
    This is expected upstream behaviour — downstream transformations handle
    these rows explicitly rather than treating them as data quality failures.
    """
    df = _get_operator_df("operator_b", run_date)

    df = df.rename(columns={"received_time": "created_at"})
    df["amount"]       = pd.to_numeric(df["amount"], errors="coerce")
    df["created_at"]   = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["_loaded_date"] = str(run_date)

    for col in ["transaction_id", "rotate_id", "msisdn", "transaction_type", "currency"]:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    result = validate_dataframe(df, "raw_operator_b", REQUIRED_COLUMNS["raw_operator_b"])
    warehouse.write_table(
        df, layer="raw", table="raw_operator_b",
        partition_date=run_date, mode="overwrite_partitions",
    )
    return result


# ---------------------------------------------------------------------------
# Operator C
# ---------------------------------------------------------------------------

def load_operator_c(warehouse: AWSWarehouse, run_date: date) -> dict:
    """
    Load operator_C source file into the raw_operator_c staging table.

    Source columns : message_id, tracking_code, msisdn, delivery_status,
                     service_id, received_time

    Semantics:
        A DELIVERED event represents both a subscription and a billing charge
        in a single record — operator C combines these into one SMS confirmation.

    Known data quality issue:
        Approximately 13% of DELIVERED rows carry a tracking_code longer than
        3 characters. This is caused by a suffix-appending bug in the operator's
        SMS parser. These rows are flagged here and quarantined downstream into
        fct_unattributed_events — the pipeline does not crash on them.
    """
    df = _get_operator_df("operator_c", run_date)

    df["received_time"] = pd.to_datetime(df["received_time"], errors="coerce", utc=True)
    df["_loaded_date"]  = str(run_date)

    for col in ["message_id", "tracking_code", "msisdn", "delivery_status", "service_id"]:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    # Detect and log rows affected by the SMS parser suffix bug.
    # These are not dropped here — quarantine happens in build_fct_subscriptions.
    bad_mask = (
        (df["delivery_status"] == "DELIVERED") &
        (df["tracking_code"].str.len() > 3)
    )
    bad_count = int(bad_mask.sum())
    if bad_count > 0:
        logger.warning(
            "[raw_operator_c] %d DELIVERED rows have a tracking_code longer than "
            "3 characters (operator SMS parser suffix bug) — these will be "
            "quarantined into fct_unattributed_events during transformation.",
            bad_count,
        )

    result = validate_dataframe(df, "raw_operator_c", REQUIRED_COLUMNS["raw_operator_c"])
    warehouse.write_table(
        df, layer="raw", table="raw_operator_c",
        partition_date=run_date, mode="overwrite_partitions",
    )
    return result


# ---------------------------------------------------------------------------
# Static reference files
# ---------------------------------------------------------------------------

def load_static_files(warehouse: AWSWarehouse) -> dict:
    """
    Full-refresh load for all static reference tables.

    These tables are small and may be updated at any time independently of
    the daily operator files, so the safest strategy is a full truncate-and-
    reload on every pipeline run.

    AWS write mode is "overwrite" (not "overwrite_partitions") because static
    tables have no date partition column — the entire table is replaced.
    """
    results = {}

    # -- Campaigns -----------------------------------------------------------
    df_campaigns = _get_static_df("campaigns")
    # Source file uses "id" as the primary key column name; rename to the
    # canonical campaign_id used throughout the warehouse.
    df_campaigns = df_campaigns.rename(columns={"id": "campaign_id"})
    df_campaigns["country"]    = df_campaigns.get("country", "GB").fillna("GB")
    df_campaigns["created_at"] = pd.to_datetime(
        df_campaigns.get("created_at"), errors="coerce", utc=True
    )
    for col in ["campaign_id", "operator", "service_name", "service_model",
                "partner_id", "status"]:
        if col in df_campaigns.columns:
            df_campaigns[col] = df_campaigns[col].astype(str).where(
                df_campaigns[col].notna(), None
            )
    r = validate_dataframe(df_campaigns, "raw_campaigns", REQUIRED_COLUMNS["raw_campaigns"])
    warehouse.write_table(
        df_campaigns, layer="raw", table="raw_campaigns",
        partition_cols=[], mode="overwrite",
    )
    results["raw_campaigns"] = r
    logger.info("[raw_campaigns] Loaded %s rows.", f"{r['row_count']:,}")

    # -- Clicks --------------------------------------------------------------
    # clicks.csv is the attribution bridge: rotate_id -> campaign_id.
    # Every subscription and billing join passes through this table.
    df_clicks = _get_static_df("clicks")
    df_clicks = df_clicks.rename(columns={"received_time": "clicked_at"})
    df_clicks["clicked_at"] = pd.to_datetime(
        df_clicks["clicked_at"], errors="coerce", utc=True
    )
    for col in ["rotate_id", "campaign_id", "pub_id"]:
        if col in df_clicks.columns:
            df_clicks[col] = df_clicks[col].astype(str).where(df_clicks[col].notna(), None)
    r = validate_dataframe(df_clicks, "raw_clicks", REQUIRED_COLUMNS["raw_clicks"])
    warehouse.write_table(
        df_clicks, layer="raw", table="raw_clicks",
        partition_cols=[], mode="overwrite",
    )
    results["raw_clicks"] = r
    logger.info("[raw_clicks] Loaded %s rows.", f"{r['row_count']:,}")

    # -- Tracking codes ------------------------------------------------------
    # Used exclusively for operator C attribution: maps a short tracking_code
    # (1-3 chars) back to a rotate_id within a 30-minute validity window
    # (created_at to expired_at).
    df_tc = _get_static_df("tracking_codes")
    df_tc["created_at"] = pd.to_datetime(df_tc.get("created_at"), errors="coerce", utc=True)
    df_tc["expired_at"] = pd.to_datetime(df_tc.get("expired_at"), errors="coerce", utc=True)
    for col in ["rotate_id", "code", "service_id"]:
        if col in df_tc.columns:
            df_tc[col] = df_tc[col].astype(str).where(df_tc[col].notna(), None)
    r = validate_dataframe(df_tc, "raw_tracking_codes")
    warehouse.write_table(
        df_tc, layer="raw", table="raw_tracking_codes",
        partition_cols=[], mode="overwrite",
    )
    results["raw_tracking_codes"] = r
    logger.info("[raw_tracking_codes] Loaded %s rows.", f"{r['row_count']:,}")

    # -- Page events ---------------------------------------------------------
    # Funnel-stage events (VIEW, CLICK_CTA, ENTRY) keyed on rotate_id.
    # Used to populate the has_page_view / has_cta_click / has_entry flags
    # in fct_clicks.
    df_pe = _get_static_df("page_events")
    df_pe = df_pe.rename(columns={"received_time": "created_at"})
    df_pe["event_type"] = df_pe["event_type"].str.upper().str.strip()
    df_pe["created_at"] = pd.to_datetime(df_pe["created_at"], errors="coerce", utc=True)
    for col in ["event_id", "rotate_id", "campaign_id", "msisdn", "device_type"]:
        if col in df_pe.columns:
            df_pe[col] = df_pe[col].astype(str).where(df_pe[col].notna(), None)
    r = validate_dataframe(df_pe, "raw_page_events")
    warehouse.write_table(
        df_pe, layer="raw", table="raw_page_events",
        partition_cols=[], mode="overwrite",
    )
    results["raw_page_events"] = r
    logger.info("[raw_page_events] Loaded %s rows.", f"{r['row_count']:,}")

    return results