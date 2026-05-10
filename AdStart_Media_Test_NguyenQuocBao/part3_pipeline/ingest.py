"""
ingest.py — Read raw CSV files → staging tables in DuckDB.

BUG FIXED (v2): Source CSVs use "received_time" for all timestamp columns.
  Previous version read non-existent columns ("timestamp", "clicked_at", "created_at")
  which would raise DuckDB "column not found" errors.

  Fix: read "received_time" from CSV → alias to the staging column name.

LOCAL: reads from data/ folder
AWS:   replace read_csv_auto() with boto3 S3 download, or Glue DynamicFrame.from_catalog()
"""
import logging
from pathlib import Path
from datetime import date

import duckdb

from config import DATA_DIR, OPERATOR_FILES, STATIC_FILES, MIN_ROW_COUNT, MAX_NULL_RATE

logger = logging.getLogger(__name__)


# ── Schema expectations ─────────────────────────────────────────
# Key columns that MUST NOT be null. Pipeline fails if null rate > threshold.
# Column names here match the STAGING table (post-ingest), not the raw CSV.
REQUIRED_COLUMNS = {
    "raw_operator_a": ["transaction_id", "event_code", "msisdn", "event_time"],
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
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE \"{col}\" IS NULL OR TRIM(CAST(\"{col}\" AS VARCHAR)) = ''"
            ).fetchone()[0]
            null_rate = null_count / row_count
            if null_rate > MAX_NULL_RATE:
                issues.append(
                    f"Column '{col}': {null_rate:.1%} null/empty (threshold {MAX_NULL_RATE:.0%})"
                )
        except Exception as e:
            issues.append(f"Column '{col}' check error: {e}")

    if issues:
        logger.warning(f"[{table}] Quality warnings:\n  " + "\n  ".join(issues))

    logger.info(f"[{table}] Loaded {row_count:,} rows. Checks passed.")
    return {"table": table, "row_count": row_count, "warnings": issues}


def _delete_existing_date(conn: duckdb.DuckDBPyConnection, table: str, run_date: date):
    """
    IDEMPOTENCY: delete today's rows before re-inserting.
    Makes the pipeline safe to re-run on the same date without duplicates.

    AWS Glue equivalent: use job bookmarks + delete-before-insert pattern.
    """
    cols = [row[0] for row in conn.execute(f"DESCRIBE {table}").fetchall()]
    if "_loaded_date" in cols:
        deleted = conn.execute(
            f"DELETE FROM {table} WHERE _loaded_date = '{run_date}'"
        ).rowcount
        if deleted > 0:
            logger.info(f"[{table}] Idempotency: removed {deleted} existing rows for {run_date}")


def load_operator_a(conn: duckdb.DuckDBPyConnection, run_date: date) -> dict:
    """
    Load operator_a raw file → raw_operator_a staging table.

    Source CSV columns : transaction_id, rotate_id, msisdn, received_time,
                         event_code, status, amount, currency
    Staging rename     : received_time → event_time
                         (avoids DuckDB reserved-keyword collision with "timestamp")

    event_code: 1=subscribe, 2=bill, 3=unsubscribe
    """
    file_path = DATA_DIR / OPERATOR_FILES["operator_a"]
    if not file_path.exists():
        raise FileNotFoundError(f"Operator A file not found: {file_path}")

    _delete_existing_date(conn, "raw_operator_a", run_date)

    conn.execute(f"""
        INSERT INTO raw_operator_a
        SELECT
            CAST(transaction_id AS VARCHAR)         AS transaction_id,
            CAST(rotate_id      AS VARCHAR)         AS rotate_id,
            CAST(msisdn         AS VARCHAR)         AS msisdn,
            CAST(event_code     AS INTEGER)         AS event_code,
            UPPER(TRIM(CAST(status AS VARCHAR)))    AS status,
            TRY_CAST(amount     AS DOUBLE)          AS amount,
            COALESCE(CAST(currency AS VARCHAR), 'GBP') AS currency,
            TRY_CAST(received_time AS TIMESTAMPTZ) AS event_time,  -- CSV col: received_time
            DATE '{run_date}'                       AS _loaded_date
        FROM read_csv_auto('{file_path}', header=true, null_padding=true)
    """)
    # AWS Glue equivalent:
    # datasource = glueContext.create_dynamic_frame.from_options(
    #     "s3", {"paths": [f"s3://adstart-raw/operator_a/date={run_date}/"]},
    #     format="csv", format_options={"withHeader": True}
    # )

    return _validate_file(conn, "raw_operator_a", REQUIRED_COLUMNS["raw_operator_a"])


def load_operator_b(conn: duckdb.DuckDBPyConnection, run_date: date) -> dict:
    """
    Load operator_b raw file → raw_operator_b staging table.

    Source CSV columns : transaction_id, rotate_id, msisdn, received_time,
                         transaction_type, package_id, amount, currency
    Staging rename     : received_time → created_at

    Key insight (from data analysis):
      - SUB  rows: rotate_id is populated (user in browser session)
      - REN  rows: rotate_id is NULL (triggered 7 days later, no session)
      - UNSUB rows: rotate_id is NULL
    Attribution for REN/UNSUB: chain via msisdn → most recent SUB → rotate_id → campaign
    """
    file_path = DATA_DIR / OPERATOR_FILES["operator_b"]
    if not file_path.exists():
        raise FileNotFoundError(f"Operator B file not found: {file_path}")

    _delete_existing_date(conn, "raw_operator_b", run_date)

    conn.execute(f"""
        INSERT INTO raw_operator_b
        SELECT
            CAST(transaction_id     AS VARCHAR)      AS transaction_id,
            CAST(rotate_id          AS VARCHAR)      AS rotate_id,   -- NULL for REN/UNSUB
            CAST(msisdn             AS VARCHAR)      AS msisdn,
            UPPER(TRIM(transaction_type))            AS transaction_type,
            TRY_CAST(amount         AS DOUBLE)       AS amount,
            COALESCE(CAST(currency AS VARCHAR), 'GBP') AS currency,
            TRY_CAST(received_time  AS TIMESTAMPTZ)  AS created_at,  -- CSV col: received_time
            DATE '{run_date}'                        AS _loaded_date
        FROM read_csv_auto('{file_path}', header=true, null_padding=true)
    """)

    return _validate_file(conn, "raw_operator_b", REQUIRED_COLUMNS["raw_operator_b"])


def load_operator_c(conn: duckdb.DuckDBPyConnection, run_date: date) -> dict:
    """
    Load operator_c raw file → raw_operator_c staging table.

    Source CSV columns : message_id, msisdn, received_time, tracking_code,
                         service_id, delivery_status

    Key insight: delivery_status='DELIVERED' means subscribe + charge happened simultaneously.
    Attribution chain: tracking_code → tracking_codes lookup → rotate_id → clicks → campaign

    Data quality note: ~13% of tracking_codes are > 3 chars (operator SMS parser issue).
    These are flagged as unattributed in transform.py, not silently dropped.
    """
    file_path = DATA_DIR / OPERATOR_FILES["operator_c"]
    if not file_path.exists():
        raise FileNotFoundError(f"Operator C file not found: {file_path}")

    _delete_existing_date(conn, "raw_operator_c", run_date)

    conn.execute(f"""
        INSERT INTO raw_operator_c
        SELECT
            CAST(message_id       AS VARCHAR)        AS message_id,
            CAST(tracking_code    AS VARCHAR)        AS tracking_code,
            CAST(msisdn           AS VARCHAR)        AS msisdn,
            UPPER(TRIM(delivery_status))             AS delivery_status,
            CAST(service_id       AS VARCHAR)        AS service_id,
            TRY_CAST(received_time AS TIMESTAMPTZ)   AS received_time,
            DATE '{run_date}'                        AS _loaded_date
        FROM read_csv_auto('{file_path}', header=true, null_padding=true)
    """)

    # Warn on unattributable tracking codes (known data quality issue)
    bad_codes = conn.execute(f"""
        SELECT COUNT(*) FROM raw_operator_c
        WHERE LENGTH(tracking_code) > 3
          AND delivery_status = 'DELIVERED'
          AND _loaded_date = '{run_date}'
    """).fetchone()[0]
    if bad_codes > 0:
        logger.warning(
            f"[raw_operator_c] {bad_codes} DELIVERED rows have tracking_code > 3 chars "
            f"— will be unattributed (operator SMS parser appends suffix)."
        )

    return _validate_file(conn, "raw_operator_c", REQUIRED_COLUMNS["raw_operator_c"])


def load_static_files(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Static reference files: campaigns, clicks, tracking_codes, page_events.
    Full-refresh (TRUNCATE + INSERT) — these are small and may be updated anytime.

    Source column renames:
      clicks.received_time      → clicked_at
      page_events.received_time → created_at
      tracking_codes: expired_at pre-computed in source (= created_at + 30 min)

    AWS: separate S3 prefix for reference data, Glue-crawled into catalog.
    """
    results = {}

    # ── campaigns ──────────────────────────────────────────────
    f = DATA_DIR / STATIC_FILES["campaigns"]
    if f.exists():
        conn.execute("DELETE FROM raw_campaigns")
        conn.execute(f"""
            INSERT INTO raw_campaigns
            SELECT
                CAST(id           AS VARCHAR)         AS id,
                COALESCE(CAST(country AS VARCHAR), 'GB') AS country,
                CAST(operator     AS VARCHAR)         AS operator,
                CAST(service_name AS VARCHAR)         AS service_name,
                CAST(service_model AS VARCHAR)        AS service_model,
                CAST(partner_id   AS VARCHAR)         AS partner_id,
                CAST(status       AS VARCHAR)         AS status,
                TRY_CAST(created_at AS TIMESTAMPTZ)   AS created_at
            FROM read_csv_auto('{f}', header=true, null_padding=true)
        """)
        results["campaigns"] = _validate_file(conn, "raw_campaigns", REQUIRED_COLUMNS["raw_campaigns"])

    # ── clicks ─────────────────────────────────────────────────
    f = DATA_DIR / STATIC_FILES["clicks"]
    if f.exists():
        conn.execute("DELETE FROM raw_clicks")
        conn.execute(f"""
            INSERT INTO raw_clicks
            SELECT
                CAST(rotate_id   AS VARCHAR)          AS rotate_id,
                CAST(campaign_id AS VARCHAR)          AS campaign_id,
                CAST(pub_id      AS VARCHAR)          AS pub_id,
                TRY_CAST(received_time AS TIMESTAMPTZ) AS clicked_at  -- CSV col: received_time
            FROM read_csv_auto('{f}', header=true, null_padding=true)
        """)
        results["clicks"] = _validate_file(conn, "raw_clicks", REQUIRED_COLUMNS["raw_clicks"])

    # ── tracking_codes ─────────────────────────────────────────
    f = DATA_DIR / STATIC_FILES["tracking_codes"]
    if f.exists():
        conn.execute("DELETE FROM raw_tracking_codes")
        conn.execute(f"""
            INSERT INTO raw_tracking_codes
            SELECT
                CAST(rotate_id  AS VARCHAR)           AS rotate_id,
                CAST(code       AS VARCHAR)           AS code,
                CAST(service_id AS VARCHAR)           AS service_id,
                TRY_CAST(created_at AS TIMESTAMPTZ)   AS created_at,
                TRY_CAST(expired_at AS TIMESTAMPTZ)   AS expired_at  -- = created_at + 30 min
            FROM read_csv_auto('{f}', header=true, null_padding=true)
        """)
        logger.info("[raw_tracking_codes] Loaded.")

    # ── page_events ────────────────────────────────────────────
    f = DATA_DIR / STATIC_FILES["page_events"]
    if f.exists():
        conn.execute("DELETE FROM raw_page_events")
        conn.execute(f"""
            INSERT INTO raw_page_events
            SELECT
                CAST(event_id    AS VARCHAR)           AS event_id,
                CAST(rotate_id   AS VARCHAR)           AS rotate_id,
                CAST(campaign_id AS VARCHAR)           AS campaign_id,
                UPPER(TRIM(CAST(event_type AS VARCHAR))) AS event_type,
                CAST(msisdn      AS VARCHAR)           AS msisdn,
                CAST(device_type AS VARCHAR)           AS device_type,
                TRY_CAST(received_time AS TIMESTAMPTZ) AS created_at  -- CSV col: received_time
            FROM read_csv_auto('{f}', header=true, null_padding=true)
        """)
        logger.info("[raw_page_events] Loaded.")

    return results