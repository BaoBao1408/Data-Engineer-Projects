"""
src/ingest/loaders.py — Load raw files vào warehouse layer.

LOCAL mode : đọc CSV từ data/raw/ → pandas DataFrame → DuckDB
AWS mode   : đọc CSV từ S3 → pandas DataFrame → S3 Parquet + Glue Catalog

Mỗi loader:
  1. Kiểm tra file tồn tại (S3 hoặc local)
  2. Đọc CSV → pandas DataFrame
  3. Rename + cast columns cho đúng schema
  4. Validate (null rate + row count)
  5. Ghi vào warehouse (DuckDB hoặc S3 Parquet)
  6. Trả về result dict để pipeline log

AWS S3 layout:
  s3://adstart-raw-<account>/
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


# ── Internal helpers ─────────────────────────────────────────────

def _read_csv_local(filename: str) -> pd.DataFrame:
    """Đọc CSV từ local data/raw/."""
    path = settings.data_dir / filename
    if not path.exists():
        raise FileNotFoundError(f"File không tìm thấy: {path}")
    return pd.read_csv(path, low_memory=False)


def _read_csv_s3(bucket: str, key: str) -> pd.DataFrame:
    """Đọc CSV từ S3 key cụ thể."""
    import boto3
    s3 = boto3.client("s3", region_name=settings.aws_region)
    logger.debug(f"Reading s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()), low_memory=False)


def _read_all_csvs_in_prefix(bucket: str, prefix: str) -> pd.DataFrame:
    """
    Đọc tất cả CSV files trong một S3 prefix.
    Hữu ích khi operator split data thành nhiều files.
    """
    import boto3
    s3 = boto3.client("s3", region_name=settings.aws_region)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".csv")]
    if not keys:
        raise FileNotFoundError(f"Không có CSV file nào tại s3://{bucket}/{prefix}")
    frames = [_read_csv_s3(bucket, k) for k in keys]
    return pd.concat(frames, ignore_index=True)


def _get_operator_df(operator_key: str, run_date: date) -> pd.DataFrame:
    """
    Router: trả về DataFrame cho operator file.
    LOCAL → đọc từ data/raw/
    AWS   → đọc từ s3://raw-bucket/operator_x/date=YYYY-MM-DD/
    """
    if settings.is_aws:
        prefix = f"{settings.operator_s3_prefixes[operator_key]}/date={run_date}/"
        return _read_all_csvs_in_prefix(settings.raw_bucket, prefix)
    else:
        filename = settings.operator_files[operator_key]
        return _read_csv_local(filename)


def _get_static_df(file_key: str) -> pd.DataFrame:
    """
    Router: trả về DataFrame cho static reference file.
    LOCAL → đọc từ data/raw/
    AWS   → đọc từ s3://raw-bucket/static/<filename>
    """
    if settings.is_aws:
        key = f"{settings.static_s3_prefix}/{settings.static_files[file_key]}"
        return _read_csv_s3(settings.raw_bucket, key)
    else:
        return _read_csv_local(settings.static_files[file_key])


# ── Operator A ───────────────────────────────────────────────────

def load_operator_a(warehouse: AWSWarehouse, run_date: date) -> dict:
    """
    Load operator_a → raw_operator_a.

    Source columns : transaction_id, rotate_id, msisdn, event_code,
                     status, amount, currency, received_time
    Staged columns : ...same + event_time (renamed), _loaded_date (added)

    event_code: 1=subscribe, 2=bill, 3=unsubscribe
    """
    df = _get_operator_df("operator_a", run_date)

    # Rename: received_time → event_time (tránh reserved keyword trong DuckDB/Athena)
    df = df.rename(columns={"received_time": "event_time"})

    # Cast types
    df["event_code"]  = pd.to_numeric(df["event_code"], errors="coerce").astype("Int64")
    df["amount"]      = pd.to_numeric(df["amount"], errors="coerce")
    df["event_time"]  = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
    df["_loaded_date"] = str(run_date)

    # Cast all varchar columns
    for col in ["transaction_id", "rotate_id", "msisdn", "status", "currency"]:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    result = validate_dataframe(df, "raw_operator_a", REQUIRED_COLUMNS["raw_operator_a"])
    warehouse.write_table(df, layer="raw", table="raw_operator_a",
                          partition_date=run_date, mode="overwrite_partitions")
    return result


# ── Operator B ───────────────────────────────────────────────────

def load_operator_b(warehouse: AWSWarehouse, run_date: date) -> dict:
    """
    Load operator_b → raw_operator_b.

    Source: transaction_id, rotate_id, msisdn, transaction_type,
            amount, currency, received_time
    Note: REN/UNSUB rows có rotate_id = NULL (by design — xử lý ở downstream)
    """
    df = _get_operator_df("operator_b", run_date)

    df = df.rename(columns={"received_time": "created_at"})
    df["amount"]      = pd.to_numeric(df["amount"], errors="coerce")
    df["created_at"]  = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["_loaded_date"] = str(run_date)

    for col in ["transaction_id", "rotate_id", "msisdn", "transaction_type", "currency"]:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    result = validate_dataframe(df, "raw_operator_b", REQUIRED_COLUMNS["raw_operator_b"])
    warehouse.write_table(df, layer="raw", table="raw_operator_b",
                          partition_date=run_date, mode="overwrite_partitions")
    return result


# ── Operator C ───────────────────────────────────────────────────

def load_operator_c(warehouse: AWSWarehouse, run_date: date) -> dict:
    """
    Load operator_c → raw_operator_c.

    Source: message_id, tracking_code, msisdn, delivery_status,
            service_id, received_time
    DELIVERED = subscribe + charge in one event.
    ~13% tracking_codes > 3 chars (SMS parser bug) — logged, không crash.
    """
    df = _get_operator_df("operator_c", run_date)

    df["received_time"] = pd.to_datetime(df["received_time"], errors="coerce", utc=True)
    df["_loaded_date"]  = str(run_date)

    for col in ["message_id", "tracking_code", "msisdn", "delivery_status", "service_id"]:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    # Log bad tracking codes (SMS parser suffix bug)
    bad_mask = (
        (df["delivery_status"] == "DELIVERED") &
        (df["tracking_code"].str.len() > 3)
    )
    bad_count = bad_mask.sum()
    if bad_count > 0:
        logger.warning(
            f"[raw_operator_c] {bad_count} DELIVERED rows có tracking_code > 3 chars "
            f"— sẽ được quarantine vào fct_unattributed_events (operator SMS parser suffix bug)."
        )

    result = validate_dataframe(df, "raw_operator_c", REQUIRED_COLUMNS["raw_operator_c"])
    warehouse.write_table(df, layer="raw", table="raw_operator_c",
                          partition_date=run_date, mode="overwrite_partitions")
    return result


# ── Static reference files ────────────────────────────────────────

def load_static_files(warehouse: AWSWarehouse) -> dict:
    """
    Full-refresh load cho static reference tables.
    Những bảng này nhỏ và có thể update bất cứ lúc nào → truncate + reload.

    AWS: dùng mode="overwrite" thay vì "overwrite_partitions"
         vì static tables không có date partition.
    """
    results = {}

    # ── Campaigns ────────────────────────────────────────────────
    df_campaigns = _get_static_df("campaigns")
    df_campaigns = df_campaigns.rename(columns={"id": "campaign_id"})
    df_campaigns["country"]     = df_campaigns.get("country", "GB").fillna("GB")
    df_campaigns["created_at"]  = pd.to_datetime(
        df_campaigns.get("created_at"), errors="coerce", utc=True
    )
    for col in ["campaign_id", "operator", "service_name", "service_model",
                "partner_id", "status"]:
        if col in df_campaigns.columns:
            df_campaigns[col] = df_campaigns[col].astype(str).where(
                df_campaigns[col].notna(), None
            )
    r = validate_dataframe(df_campaigns, "raw_campaigns", REQUIRED_COLUMNS["raw_campaigns"])
    warehouse.write_table(df_campaigns, layer="raw", table="raw_campaigns",
                          partition_cols=[], mode="overwrite")
    results["raw_campaigns"] = r
    logger.info(f"[raw_campaigns] Loaded {r['row_count']:,} rows.")

    # ── Clicks ───────────────────────────────────────────────────
    df_clicks = _get_static_df("clicks")
    df_clicks = df_clicks.rename(columns={"received_time": "clicked_at"})
    df_clicks["clicked_at"] = pd.to_datetime(df_clicks["clicked_at"], errors="coerce", utc=True)
    for col in ["rotate_id", "campaign_id", "pub_id"]:
        if col in df_clicks.columns:
            df_clicks[col] = df_clicks[col].astype(str).where(df_clicks[col].notna(), None)
    r = validate_dataframe(df_clicks, "raw_clicks", REQUIRED_COLUMNS["raw_clicks"])
    warehouse.write_table(df_clicks, layer="raw", table="raw_clicks",
                          partition_cols=[], mode="overwrite")
    results["raw_clicks"] = r
    logger.info(f"[raw_clicks] Loaded {r['row_count']:,} rows.")

    # ── Tracking Codes ───────────────────────────────────────────
    df_tc = _get_static_df("tracking_codes")
    df_tc["created_at"] = pd.to_datetime(df_tc.get("created_at"), errors="coerce", utc=True)
    df_tc["expired_at"] = pd.to_datetime(df_tc.get("expired_at"), errors="coerce", utc=True)
    for col in ["rotate_id", "code", "service_id"]:
        if col in df_tc.columns:
            df_tc[col] = df_tc[col].astype(str).where(df_tc[col].notna(), None)
    r = validate_dataframe(df_tc, "raw_tracking_codes")
    warehouse.write_table(df_tc, layer="raw", table="raw_tracking_codes",
                          partition_cols=[], mode="overwrite")
    results["raw_tracking_codes"] = r
    logger.info(f"[raw_tracking_codes] Loaded {r['row_count']:,} rows.")

    # ── Page Events ──────────────────────────────────────────────
    df_pe = _get_static_df("page_events")
    df_pe = df_pe.rename(columns={"received_time": "created_at"})
    df_pe["event_type"] = df_pe["event_type"].str.upper().str.strip()
    df_pe["created_at"] = pd.to_datetime(df_pe["created_at"], errors="coerce", utc=True)
    for col in ["event_id", "rotate_id", "campaign_id", "msisdn", "device_type"]:
        if col in df_pe.columns:
            df_pe[col] = df_pe[col].astype(str).where(df_pe[col].notna(), None)
    r = validate_dataframe(df_pe, "raw_page_events")
    warehouse.write_table(df_pe, layer="raw", table="raw_page_events",
                          partition_cols=[], mode="overwrite")
    results["raw_page_events"] = r
    logger.info(f"[raw_page_events] Loaded {r['row_count']:,} rows.")

    return results
