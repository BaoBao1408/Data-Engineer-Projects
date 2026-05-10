"""
src/ingest/loaders.py — Load raw CSV files into DuckDB staging tables.

Each loader:
  1. Validates source file exists
  2. Delegates SQL to sql/raw/<operator>.sql (no inline SQL here)
  3. Calls validator for quality checks
  4. Returns a result dict

AWS equivalent: each function → 1 Glue job reading from S3.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import duckdb

from config.base import settings
from src.ingest.validator import validate_table, REQUIRED_COLUMNS
from src.utils.db import run_sql_file

logger = logging.getLogger(__name__)


def _resolve_operator_file(key: str) -> Path:
    path = settings.data_dir / settings.operator_files[key]
    if not path.exists():
        raise FileNotFoundError(f"Operator file not found: {path}")
    return path


# ── Operator loaders ─────────────────────────────────────────────

def load_operator_a(conn: duckdb.DuckDBPyConnection, run_date: date) -> dict:
    """
    Load operator_a.csv → raw_operator_a.
    Source column: received_time → staged as event_time.
    event_code: 1=subscribe, 2=bill, 3=unsubscribe.
    """
    file_path = _resolve_operator_file("operator_a")
    run_sql_file(conn, "raw/operator_a.sql", {
        "run_date":  run_date,
        "file_path": file_path,
    })
    return validate_table(conn, "raw_operator_a", REQUIRED_COLUMNS["raw_operator_a"])


def load_operator_b(conn: duckdb.DuckDBPyConnection, run_date: date) -> dict:
    """
    Load operator_b.csv → raw_operator_b.
    SUB rows have rotate_id; REN/UNSUB rows have rotate_id = NULL (by design).
    REN attribution handled downstream in fct_billing via msisdn chain.
    """
    file_path = _resolve_operator_file("operator_b")
    run_sql_file(conn, "raw/operator_b.sql", {
        "run_date":  run_date,
        "file_path": file_path,
    })
    return validate_table(conn, "raw_operator_b", REQUIRED_COLUMNS["raw_operator_b"])


def load_operator_c(conn: duckdb.DuckDBPyConnection, run_date: date) -> dict:
    """
    Load operator_c.csv → raw_operator_c.
    DELIVERED = subscribe + charge combined.
    ~13% of tracking_codes are > 3 chars (SMS parser bug) — logged, not crashed.
    """
    file_path = _resolve_operator_file("operator_c")
    run_sql_file(conn, "raw/operator_c.sql", {
        "run_date":  run_date,
        "file_path": file_path,
    })

    bad_codes = conn.execute(f"""
        SELECT COUNT(*) FROM raw_operator_c
        WHERE LENGTH(tracking_code) > 3
          AND delivery_status = 'DELIVERED'
          AND _loaded_date = '{run_date}'
    """).fetchone()[0]
    if bad_codes > 0:
        logger.warning(
            f"[raw_operator_c] {bad_codes} DELIVERED rows have tracking_code > 3 chars "
            f"— will be unattributed (operator SMS parser suffix issue)."
        )

    return validate_table(conn, "raw_operator_c", REQUIRED_COLUMNS["raw_operator_c"])


# ── Static reference files ────────────────────────────────────────

def load_static_files(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Full-refresh load for static reference tables (campaigns, clicks, tracking_codes, page_events).
    These are small tables that may be updated at any time → TRUNCATE + INSERT.
    AWS: separate S3 prefix, Glue-crawled into catalog.
    """
    results: dict = {}
    sf = settings.static_files
    data = settings.data_dir

    _load_reference(conn, data / sf["campaigns"],      "raw_campaigns",      results,
        """DELETE FROM raw_campaigns;
           INSERT INTO raw_campaigns
           SELECT CAST(id AS VARCHAR), COALESCE(CAST(country AS VARCHAR),'GB'),
                  CAST(operator AS VARCHAR), CAST(service_name AS VARCHAR),
                  CAST(service_model AS VARCHAR), CAST(partner_id AS VARCHAR),
                  CAST(status AS VARCHAR), TRY_CAST(created_at AS TIMESTAMPTZ)
           FROM read_csv_auto('{f}', header=true, null_padding=true)""",
        REQUIRED_COLUMNS["raw_campaigns"])

    _load_reference(conn, data / sf["clicks"], "raw_clicks", results,
        """DELETE FROM raw_clicks;
           INSERT INTO raw_clicks
           SELECT CAST(rotate_id AS VARCHAR), CAST(campaign_id AS VARCHAR),
                  CAST(pub_id AS VARCHAR),
                  TRY_CAST(received_time AS TIMESTAMPTZ) AS clicked_at
           FROM read_csv_auto('{f}', header=true, null_padding=true)""",
        REQUIRED_COLUMNS["raw_clicks"])

    _load_reference(conn, data / sf["tracking_codes"], "raw_tracking_codes", results,
        """DELETE FROM raw_tracking_codes;
           INSERT INTO raw_tracking_codes
           SELECT CAST(rotate_id AS VARCHAR), CAST(code AS VARCHAR),
                  CAST(service_id AS VARCHAR),
                  TRY_CAST(created_at AS TIMESTAMPTZ),
                  TRY_CAST(expired_at AS TIMESTAMPTZ)
           FROM read_csv_auto('{f}', header=true, null_padding=true)""")

    _load_reference(conn, data / sf["page_events"], "raw_page_events", results,
        """DELETE FROM raw_page_events;
           INSERT INTO raw_page_events
           SELECT CAST(event_id AS VARCHAR), CAST(rotate_id AS VARCHAR),
                  CAST(campaign_id AS VARCHAR),
                  UPPER(TRIM(CAST(event_type AS VARCHAR))),
                  CAST(msisdn AS VARCHAR), CAST(device_type AS VARCHAR),
                  TRY_CAST(received_time AS TIMESTAMPTZ) AS created_at
           FROM read_csv_auto('{f}', header=true, null_padding=true)""")

    return results


def _load_reference(
    conn: duckdb.DuckDBPyConnection,
    file_path: Path,
    table: str,
    results: dict,
    sql_template: str,
    required_cols: list[str] | None = None,
) -> None:
    if not file_path.exists():
        logger.warning(f"[{table}] File not found, skipping: {file_path}")
        return
    conn.execute(sql_template.format(f=file_path))
    result = validate_table(conn, table, required_cols or [])
    results[table] = result
    logger.info(f"[{table}] Loaded {result['row_count']:,} rows.")
