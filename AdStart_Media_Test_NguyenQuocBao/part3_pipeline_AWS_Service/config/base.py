"""
config/base.py — Centralized, environment-aware settings.

Usage:
    from config.base import settings
    settings.db_path  # local DuckDB path
    settings.data_dir # raw CSV folder
"""
import os
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum
from dotenv import load_dotenv
 
load_dotenv()


class Environment(str, Enum):
    LOCAL = "local"
    AWS   = "aws"


@dataclass
class Settings:
    env: Environment = Environment.LOCAL

    # ── Paths (LOCAL) ──────────────────────────────────────────
    # AWS: replace with s3://your-bucket/raw/ + Redshift DSN
    data_dir: Path = Path("data/raw")
    db_path: Path = Path("data/warehouse/warehouse.duckdb")
    log_dir: Path = Path("logs")
    
    # ── AWS S3 buckets ─────────────────────────────────────────
    # s3://adstart-raw-<account_id>/   → raw CSV files from operator
    # s3://adstart-warehouse-<account_id>/ → Parquet warehouse layers
    raw_bucket:       str = ""   # e.g. "adstart-raw-123456789"
    warehouse_bucket: str = ""   # e.g. "adstart-warehouse-123456789"
        # S3 key prefixes inside warehouse bucket
    s3_raw_prefix:   str = "raw"        # s3://warehouse/raw/raw_operator_a/...
    s3_dim_prefix:   str = "dimensions" # s3://warehouse/dimensions/dim_campaigns/...
    s3_fact_prefix:  str = "facts"      # s3://warehouse/facts/fct_subscriptions/...
    s3_mart_prefix:  str = "mart"       # s3://warehouse/mart/mart_daily_performance/...
 
    # ── AWS Glue / Athena ──────────────────────────────────────
    # Glue Catalog databases (create table infrastructure/setup_aws.py)
    glue_raw_database:       str = "adstart_raw"       # raw Parquet tables
    glue_warehouse_database: str = "adstart_warehouse" # fact + mart tables
 
    # Athena query results save here ( awswrangler)
    athena_output_bucket: str = ""  # e.g. "adstart-athena-results-123456789"
    athena_output_prefix: str = "query-results"
 
    # ── AWS Region ─────────────────────────────────────────────
    aws_region: str = "eu-west-1"  # Ireland — near uk data

    # ── Operator file mapping ───────────────────────────────────
    # LOCAL: filename trong data_dir
    # AWS:   key prefix in raw_bucket (extra /date=YYYY-MM-DD/data.csv)
    operator_files: dict = field(default_factory=lambda: {
        "operator_a": "operator_A.csv",
        "operator_b": "operator_B.csv",
        "operator_c": "operator_C.csv",
    })
    operator_s3_prefixes: dict = field(default_factory=lambda: {
        "operator_a": "operator_a",
        "operator_b": "operator_b",
        "operator_c": "operator_c",
    })
 
    static_files: dict = field(default_factory=lambda: {
        "campaigns":      "campaigns.csv",
        "clicks":         "clicks.csv",
        "tracking_codes": "tracking_codes.csv",
        "page_events":    "page_events.csv",
    })
    static_s3_prefix: str = "static"  # s3://raw-bucket/static/campaigns.csv
 
    # ── Data quality thresholds ─────────────────────────────────
    max_null_rate: float = 0.05
    min_row_count: int   = 1
 
    # ── Notification (AWS SNS) ──────────────────────────────────
    # ARN for an SNS topic to send alerts to (e.g. data quality failures, pipeline errors)
    sns_alert_topic_arn: str = ""  # e.g. "arn:aws:sns:eu-west-1:123:adstart-alerts"
    @property
    def athena_s3_output(self) -> str:
        """Full S3 path cho Athena query results."""
        return f"s3://{self.athena_output_bucket}/{self.athena_output_prefix}/"
 
    @property
    def is_aws(self) -> bool:
        return self.env == Environment.AWS
 
    def s3_raw_table_path(self, table_name: str) -> str:
        """s3://warehouse/raw/raw_operator_a/"""
        return f"s3://{self.warehouse_bucket}/{self.s3_raw_prefix}/{table_name}/"
 
    def s3_dim_path(self, table_name: str) -> str:
        """s3://warehouse/dimensions/dim_campaigns/"""
        return f"s3://{self.warehouse_bucket}/{self.s3_dim_prefix}/{table_name}/"
 
    def s3_fact_path(self, table_name: str) -> str:
        """s3://warehouse/facts/fct_subscriptions/"""
        return f"s3://{self.warehouse_bucket}/{self.s3_fact_prefix}/{table_name}/"
 
    def s3_mart_path(self, table_name: str) -> str:
        """s3://warehouse/mart/mart_daily_performance/"""
        return f"s3://{self.warehouse_bucket}/{self.s3_mart_prefix}/{table_name}/"
 
    def s3_operator_path(self, operator_key: str, run_date) -> str:
        """s3://raw-bucket/operator_a/date=2026-01-15/data.csv"""
        prefix = self.operator_s3_prefixes[operator_key]
        return f"s3://{self.raw_bucket}/{prefix}/date={run_date}/"
 

def _load_settings() -> Settings:
    env = Environment(os.getenv("PIPELINE_ENV", "local"))
    s = Settings(env=env)
 
    if env == Environment.AWS:
        # Load from env vars (set trong EC2/ECS/Lambda environment)
        s.raw_bucket             = os.getenv("AWS_RAW_BUCKET", "")
        s.warehouse_bucket       = os.getenv("AWS_WAREHOUSE_BUCKET", "")
        s.athena_output_bucket   = os.getenv("AWS_ATHENA_OUTPUT_BUCKET", "")
        s.aws_region             = os.getenv("AWS_REGION", "eu-west-1")
        s.sns_alert_topic_arn    = os.getenv("SNS_ALERT_TOPIC_ARN", "")
        s.glue_raw_database      = os.getenv("GLUE_RAW_DATABASE", "adstart_raw")
        s.glue_warehouse_database = os.getenv("GLUE_WAREHOUSE_DATABASE", "adstart_warehouse")
 
        missing = [k for k, v in {
            "AWS_RAW_BUCKET": s.raw_bucket,
            "AWS_WAREHOUSE_BUCKET": s.warehouse_bucket,
            "AWS_ATHENA_OUTPUT_BUCKET": s.athena_output_bucket,
        }.items() if not v]
        if missing:
            raise EnvironmentError(
                f"AWS mode requires these env vars: {missing}\n"
                f"Copy .env.example → .env và write bucket names."
            )
 
    return s
 
 
settings = _load_settings()
