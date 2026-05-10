"""
config/base.py — Centralized, environment-aware settings.

Usage:
    from config.base import settings
    settings.db_path  # local DuckDB path
    settings.data_dir # raw CSV folder
"""
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum


class Environment(str, Enum):
    LOCAL = "local"
    STAGING = "staging"
    PROD = "prod"


@dataclass
class Settings:
    env: Environment = Environment.LOCAL

    # ── Paths (LOCAL) ──────────────────────────────────────────
    # AWS: replace with s3://your-bucket/raw/ + Redshift DSN
    data_dir: Path = Path("data")
    db_path: Path = Path("warehouse.duckdb")
    log_dir: Path = Path("logs")

    # ── Operator file mapping ───────────────────────────────────
    operator_files: dict = field(default_factory=lambda: {
        "operator_a": "operator_a.csv",
        "operator_b": "operator_b.csv",
        "operator_c": "operator_c.csv",
    })

    static_files: dict = field(default_factory=lambda: {
        "campaigns":      "campaigns.csv",
        "clicks":         "clicks.csv",
        "tracking_codes": "tracking_codes.csv",
        "page_events":    "page_events.csv",
    })

    # ── Data quality thresholds ─────────────────────────────────
    max_null_rate: float = 0.05   # fail if key column > 5% null
    min_row_count: int   = 1      # fail if file has 0 rows


def _load_settings() -> Settings:
    import os
    env = Environment(os.getenv("PIPELINE_ENV", "local"))
    return Settings(env=env)


settings = _load_settings()
