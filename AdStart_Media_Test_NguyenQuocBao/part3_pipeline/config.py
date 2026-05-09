"""
config.py — Centralized settings for the pipeline.

LOCAL  → paths on disk, DuckDB file
AWS    → replace these with boto3 S3 paths + Glue job params
"""
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timezone


# ── Paths (LOCAL) ──────────────────────────────────────────────
DATA_DIR   = Path("data")          # AWS → s3://your-bucket/raw/
DB_PATH    = Path("warehouse.duckdb")  # AWS → Redshift / Athena + S3 Parquet
LOG_DIR    = Path("logs")

# ── Operator file mapping ───────────────────────────────────────
# Each operator delivers 1 file per day.
# AWS: these arrive in S3, trigger Lambda → Step Functions
OPERATOR_FILES = {
    "operator_a": "operator_a.csv",
    "operator_b": "operator_b.csv",
    "operator_c": "operator_c.csv",
}

STATIC_FILES = {
    "campaigns"      : "campaigns.csv",
    "clicks"         : "clicks.csv",
    "tracking_codes" : "tracking_codes.csv",
    "page_events"    : "page_events.csv",
}

# ── Pipeline run tracking ───────────────────────────────────────
PIPELINE_RUNS_TABLE = "pipeline_runs"

# ── Retry / quality thresholds ──────────────────────────────────
MAX_NULL_RATE       = 0.05   # fail if any key column > 5% null
MIN_ROW_COUNT       = 1      # fail if a file has 0 rows
