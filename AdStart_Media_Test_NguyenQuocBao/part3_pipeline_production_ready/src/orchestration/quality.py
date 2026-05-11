"""
src/orchestration/quality.py — Final data quality gate.

Runs SQL assertions against mart tables and the quarantine table.
Raises ValueError if any check fails.
AWS: Lambda triggered after Glue jobs complete; alerts via SNS on failure.

Quality files executed (each must return 0 rows to pass):
  1. quality/check_mart.sql          — mart row count, negative revenue, rate bounds
  2. quality/check_duplicates.sql    — PK uniqueness in fact tables
  3. quality/check_attribution_rate.sql — Layer 3: operator_C unattributed rate alert
"""
from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).parent.parent.parent / "sql"

# All quality check files, executed in order.
# Each file may contain multiple statements separated by ;
_QUALITY_FILES = [
    "quality/check_mart.sql",
    "quality/check_duplicates.sql",
    "quality/check_attribution_rate.sql",   # Layer 3
]


def _split_sql_statements(sql: str) -> list[str]:
    """
    Split a SQL file into individual statements, ignoring semicolons inside comments.
    Filters out comment-only or blank fragments that would cause DuckDB parse errors.
    """
    # Split on ; that are NOT inside a -- comment line
    statements = []
    for raw in re.split(r";\s*\n", sql):
        stmt = raw.strip()
        if not stmt:
            continue
        # Remove comment lines to check if anything executable remains
        non_comment = "\n".join(
            line for line in stmt.splitlines()
            if not line.strip().startswith("--")
        ).strip()
        if non_comment:
            statements.append(stmt)
    return statements


def run_quality_checks(conn: duckdb.DuckDBPyConnection, run_date: date) -> bool:
    """
    Execute all quality SQL files and raise ValueError on any assertion failure.
    Returns True if all checks pass.

    A check PASSES when its SELECT returns 0 rows (nothing failing).
    A check FAILS  when its SELECT returns ≥ 1 row (check_name + failing_rows columns).
    """
    failures: list[str] = []

    for sql_file in _QUALITY_FILES:
        sql_path = _SQL_DIR / sql_file
        if not sql_path.exists():
            logger.warning(f"[quality] Check file not found, skipping: {sql_file}")
            continue

        sql_raw = sql_path.read_text(encoding="utf-8")
        sql_rendered = sql_raw.replace(":run_date", f"'{run_date}'")

        for stmt in _split_sql_statements(sql_rendered):
            try:
                rows = conn.execute(stmt).fetchall()
                for row in rows:
                    check_name = row[0] if row else "unknown"
                    failing    = row[-1] if len(row) > 1 else 0
                    if failing and int(failing) > 0:
                        failures.append(f"[{sql_file}] {check_name}: {failing} failing row(s)")
            except Exception as exc:
                failures.append(f"[{sql_file}] Execution error: {exc}")

    if failures:
        msg = "Quality checks FAILED:\n  " + "\n  ".join(failures)
        logger.error(msg)
        # AWS: sns.publish(TopicArn=ALERT_TOPIC, Message=msg, Subject=f"Pipeline FAILED {run_date}")
        raise ValueError(msg)

    # ── Summary on success ─────────────────────────────────────
    mart_rows = conn.execute(
        f"SELECT COUNT(*) FROM mart_daily_performance WHERE report_date = '{run_date}'"
    ).fetchone()[0]

    unattr_rows = conn.execute(
        f"SELECT COUNT(*) FROM fct_unattributed_events WHERE report_date = '{run_date}'"
    ).fetchone()[0]

    attr_rate_row = conn.execute(f"""
        SELECT ROUND(AVG(attribution_rate) * 100, 1)
        FROM mart_daily_performance
        WHERE report_date = '{run_date}'
          AND attribution_rate IS NOT NULL
    """).fetchone()[0]

    attr_rate_str = f"{attr_rate_row}%" if attr_rate_row is not None else "N/A"

    logger.info(
        f"All quality checks passed for {run_date}. "
        f"Mart: {mart_rows} campaign rows | "
        f"Quarantine: {unattr_rows} unattributed event(s) | "
        f"Avg attribution rate: {attr_rate_str}"
    )
    return True
