"""
src/transformations/dimensions.py — Build dimension tables.

Each function:
  - Delegates SQL to sql/dimensions/<dim>.sql
  - Returns row count for audit logging
  - Is idempotent (INSERT OR IGNORE)

AWS equivalent: dbt snapshot or Glue upsert job.
"""
from __future__ import annotations

import logging
from datetime import date

import duckdb

from temp.src.utils.db import run_sql_file

logger = logging.getLogger(__name__)


def build_dim_campaigns(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Populate dim_campaigns from raw_campaigns.
    SCD Type 1: INSERT OR IGNORE — existing rows are never overwritten.
    """
    run_sql_file(conn, "dimensions/dim_campaigns.sql")
    count = conn.execute("SELECT COUNT(*) FROM dim_campaigns").fetchone()[0]
    logger.info(f"[dim_campaigns] {count:,} rows total.")
    return count
