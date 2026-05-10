"""
src/orchestration/quality.py — Final data quality gate.

Runs SQL assertions against mart_daily_performance.
Raises ValueError if any check fails.
AWS: Lambda triggered after Glue jobs complete; alerts via SNS on failure.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import duckdb

from temp.src.utils.db import run_sql_file

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).parent.parent.parent / "sql"


def run_quality_checks(conn: duckdb.DuckDBPyConnection, run_date: date) -> bool:
    """
    Execute all quality SQL files and raise on any failure.
    Returns True if all checks pass.
    """
    failures: list[str] = []

    for sql_file in ["quality/check_mart.sql", "quality/check_duplicates.sql"]:
        sql = (_SQL_DIR / sql_file).read_text()
        # Substitute :run_date
        sql_rendered = sql.replace(":run_date", f"'{run_date}'")

        for statement in sql_rendered.split(";"):
            stmt = statement.strip()
            if not stmt:
                continue
            try:
                rows = conn.execute(stmt).fetchall()
                for row in rows:
                    check_name = row[0] if row else "unknown"
                    failing = row[-1] if len(row) > 1 else 0
                    if failing and int(failing) > 0:
                        failures.append(f"{check_name}: {failing} failing row(s)")
            except Exception as exc:
                failures.append(f"Check error in {sql_file}: {exc}")

    if failures:
        msg = "Quality checks FAILED:\n  " + "\n  ".join(failures)
        logger.error(msg)
        # AWS: sns.publish(TopicArn=ALERT_TOPIC, Message=msg)
        raise ValueError(msg)

    mart_rows = conn.execute(
        f"SELECT COUNT(*) FROM mart_daily_performance WHERE report_date = '{run_date}'"
    ).fetchone()[0]
    logger.info(f"All quality checks passed for {run_date}. Mart has {mart_rows} campaign rows.")
    return True
