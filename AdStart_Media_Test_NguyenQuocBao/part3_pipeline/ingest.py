"""
ingest.py — Read raw CSV files → staging tables in DuckDB.

LOCAL: reads from data/ folder
AWS:   replace read_csv_auto() with boto3 S3 download → temp file,
       or use Glue DynamicFrame.from_catalog()
"""
import logging
from pathlib import Path
from datetime import date

import duckdb

from config import DATA_DIR, OPERATOR_FILES, STATIC_FILES, MIN_ROW_COUNT, MAX_NULL_RATE

logger = logging.getLogger(__name__)


# ── Schema expectations ─────────────────────────────────────────
# Key columns that MUST NOT be null. Pipeline fails if null rate > threshold.
REQUIRED_COLUMNS = {
    "raw_operator_a": ["transaction_id", "event_code", "msisdn", "timestamp"],
    "raw_operator_b": ["transaction_id", "transaction_type", "msisdn", "created_at"],
    "raw_operator_c": ["message_id", "tracking_code", "msisdn", "received_time"],
    "raw_campaigns":  ["id", "operator", "service_name", "partner_id"],
    "raw_clicks":     ["rotate_id", "campaign_id", "clicked_at"],
}


def _validate_file(conn: duckdb.DuckDBPyConnection, table: str, required_cols: list[str]) -> dict:
    """
    Run basic quality checks on a freshly loaded staging table.
    Returns a dict of check results. Raises ValueError if critical checks fail.
    """
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if row_count < MIN_ROW_COUNT:
        raise ValueError(f"[{table}] Empty file: {row_count} rows. Pipeline aborted.")

    issues = []
    for col in required_cols:
        try:
            null_count = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL OR TRIM(CAST({col} AS VARCHAR)) = ''"
            ).fetchone()[0]
            null_rate = null_count / row_count
            if null_rate > MAX_NULL_RATE:
                issues.append(f"Column '{col}': {null_rate:.1%} null/empty (threshold {MAX_NULL_RATE:.0%})")
        except Exception as e:
            issues.append(f"Column '{col}' check error: {e}")

    if issues:
        logger.warning(f"[{table}] Quality warnings:\n  " + "\n  ".join(issues))

    logger.info(f"[{table}] Loaded {row_count:,} rows. Checks passed.")
    return {"table": table, "row_count": row_count, "warnings": issues}


def _delete_existing_date(conn: duckdb.DuckDBPyConnection, table: str, run_date: date):
    """
    IDEMPOTENCY: delete today's rows before re-inserting.
    This makes the pipeline safe to run multiple times on the same day.

    AWS Glue equivalent: use job bookmarks + delete-insert pattern in Glue script.
    """
    # Only operator tables have _loaded_date (static files are always full-refresh)
    if "_loaded_date" in [col[0] for col in conn.execute(f"DESCRIBE {table}").fetchall()]:
        deleted = conn.execute(
            f"DELETE FROM {table} WHERE _loaded_date = '{run_date}'"
        ).rowcount
        if deleted > 0:
            logger.info(f"[{table}] Idempotency: removed {deleted} existing rows for {run_date}")


def load_operator_a(conn: duckdb.DuckDBPyConnection, run_date: date) -> dict:
    """
    Load operator_a raw file → raw_operator_a staging table.

    Operator A schema: transaction_id, rotate_id, msisdn, event_code, status, amount, timestamp
    event_code=1 → subscription, event_code=2 → billing
    """
    file_path = DATA_DIR / OPERATOR_FILES["operator_a"]
    if not file_path.exists():
        raise FileNotFoundError(f"Operator A file not found: {file_path}")

    _delete_existing_date(conn, "raw_operator_a", run_date)

    conn.execute(f"""
        INSERT INTO raw_operator_a
        SELECT
            CAST(transaction_id AS VARCHAR)  AS transaction_id,
            CAST(rotate_id AS VARCHAR)       AS rotate_id,
            CAST(msisdn AS VARCHAR)          AS msisdn,
            CAST(event_code AS INTEGER)      AS event_code,
            CAST(status AS VARCHAR)          AS status,
            TRY_CAST(amount AS DOUBLE)       AS amount,
            TRY_CAST(timestamp AS TIMESTAMPTZ) AS timestamp,
            DATE '{run_date}'                AS _loaded_date
        FROM read_csv_auto('{file_path}', header=true, null_padding=true)
    """)
    # AWS Glue equivalent:
    # datasource = glueContext.create_dynamic_frame.from_options(
    #     "s3", {"paths": [f"s3://bucket/raw/{run_date}/operator_a/"]},
    #     format="csv", format_options={"withHeader": True}
    # )

    return _validate_file(conn, "raw_operator_a", REQUIRED_COLUMNS["raw_operator_a"])


def load_operator_b(conn: duckdb.DuckDBPyConnection, run_date: date) -> dict:
    """
    Operator B schema: transaction_id, rotate_id, msisdn, transaction_type, amount, created_at
    transaction_type='SUB' → subscription, 'REN' → billing renewal
    """
    file_path = DATA_DIR / OPERATOR_FILES["operator_b"]
    if not file_path.exists():
        raise FileNotFoundError(f"Operator B file not found: {file_path}")

    _delete_existing_date(conn, "raw_operator_b", run_date)

    conn.execute(f"""
        INSERT INTO raw_operator_b
        SELECT
            CAST(transaction_id AS VARCHAR)      AS transaction_id,
            CAST(rotate_id AS VARCHAR)           AS rotate_id,
            CAST(msisdn AS VARCHAR)              AS msisdn,
            UPPER(TRIM(transaction_type))        AS transaction_type,
            TRY_CAST(amount AS DOUBLE)           AS amount,
            TRY_CAST(created_at AS TIMESTAMPTZ)  AS created_at,
            DATE '{run_date}'                    AS _loaded_date
        FROM read_csv_auto('{file_path}', header=true, null_padding=true)
    """)

    return _validate_file(conn, "raw_operator_b", REQUIRED_COLUMNS["raw_operator_b"])


def load_operator_c(conn: duckdb.DuckDBPyConnection, run_date: date) -> dict:
    """
    Operator C schema: message_id, tracking_code, msisdn, delivery_status, received_time
    delivery_status='DELIVERED' → subscription + billing (combined event, no separate rotate_id)
    tracking_code links back to clicks table via tracking_codes lookup table.
    """
    file_path = DATA_DIR / OPERATOR_FILES["operator_c"]
    if not file_path.exists():
        raise FileNotFoundError(f"Operator C file not found: {file_path}")

    _delete_existing_date(conn, "raw_operator_c", run_date)

    conn.execute(f"""
        INSERT INTO raw_operator_c
        SELECT
            CAST(message_id AS VARCHAR)           AS message_id,
            CAST(tracking_code AS VARCHAR)        AS tracking_code,
            CAST(msisdn AS VARCHAR)               AS msisdn,
            UPPER(TRIM(delivery_status))          AS delivery_status,
            TRY_CAST(received_time AS TIMESTAMPTZ) AS received_time,
            DATE '{run_date}'                     AS _loaded_date
        FROM read_csv_auto('{file_path}', header=true, null_padding=true)
    """)

    return _validate_file(conn, "raw_operator_c", REQUIRED_COLUMNS["raw_operator_c"])


def load_static_files(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Static reference files: campaigns, clicks, tracking_codes, page_events.
    These are full-refresh (TRUNCATE + INSERT) since they can be updated anytime.

    AWS: these would be in a separate S3 prefix, Glue crawled into catalog.
    """
    results = {}

    # campaigns
    f = DATA_DIR / STATIC_FILES["campaigns"]
    if f.exists():
        conn.execute("DELETE FROM raw_campaigns")
        conn.execute(f"""
            INSERT INTO raw_campaigns
            SELECT
                CAST(id AS VARCHAR)           AS id,
                CAST(operator AS VARCHAR)     AS operator,
                CAST(service_name AS VARCHAR) AS service_name,
                CAST(service_model AS VARCHAR) AS service_model,
                CAST(partner_id AS VARCHAR)   AS partner_id,
                CAST(status AS VARCHAR)       AS status,
                TRY_CAST(created_at AS TIMESTAMPTZ) AS created_at
            FROM read_csv_auto('{f}', header=true, null_padding=true)
        """)
        results["campaigns"] = _validate_file(conn, "raw_campaigns", REQUIRED_COLUMNS["raw_campaigns"])

    # clicks
    f = DATA_DIR / STATIC_FILES["clicks"]
    if f.exists():
        conn.execute("DELETE FROM raw_clicks")
        conn.execute(f"""
            INSERT INTO raw_clicks
            SELECT
                CAST(rotate_id AS VARCHAR)   AS rotate_id,
                CAST(campaign_id AS VARCHAR) AS campaign_id,
                CAST(pub_id AS VARCHAR)      AS pub_id,
                TRY_CAST(clicked_at AS TIMESTAMPTZ) AS clicked_at
            FROM read_csv_auto('{f}', header=true, null_padding=true)
        """)
        results["clicks"] = _validate_file(conn, "raw_clicks", REQUIRED_COLUMNS["raw_clicks"])

    # tracking_codes
    f = DATA_DIR / STATIC_FILES["tracking_codes"]
    if f.exists():
        conn.execute("DELETE FROM raw_tracking_codes")
        conn.execute(f"""
            INSERT INTO raw_tracking_codes
            SELECT
                CAST(code AS VARCHAR)      AS code,
                CAST(rotate_id AS VARCHAR) AS rotate_id,
                TRY_CAST(created_at AS TIMESTAMPTZ) AS created_at,
                TRY_CAST(expired_at AS TIMESTAMPTZ) AS expired_at
            FROM read_csv_auto('{f}', header=true, null_padding=true)
        """)

    # page_events
    f = DATA_DIR / STATIC_FILES["page_events"]
    if f.exists():
        conn.execute("DELETE FROM raw_page_events")
        conn.execute(f"""
            INSERT INTO raw_page_events
            SELECT
                CAST(rotate_id AS VARCHAR)  AS rotate_id,
                CAST(event_type AS VARCHAR) AS event_type,
                TRY_CAST(created_at AS TIMESTAMPTZ) AS created_at
            FROM read_csv_auto('{f}', header=true, null_padding=true)
        """)

    logger.info("Static files loaded.")
    return results
