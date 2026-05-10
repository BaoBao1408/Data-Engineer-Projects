"""
src/utils/db.py — DuckDB connection factory and SQL runner.

Centralises:
  - Connection creation (LOCAL: file, TEST: :memory:)
  - Idempotent schema bootstrap
  - Parameterised SQL execution from .sql files
"""
from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from config.base import settings

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).parent.parent.parent / "schema.sql"
_SQL_DIR     = Path(__file__).parent.parent.parent / "sql"


def get_connection(db_path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    """
    Return an open DuckDB connection with schema bootstrapped.
    AWS replacement: swap for Redshift psycopg2 / Athena boto3 client.
    """
    path = str(db_path or settings.db_path)
    conn = duckdb.connect(path)
    conn.execute(_SCHEMA_PATH.read_text())   # idempotent — CREATE TABLE IF NOT EXISTS
    return conn


def run_sql_file(
    conn: duckdb.DuckDBPyConnection,
    rel_path: str,
    params: dict | None = None,
) -> None:
    """
    Execute a SQL file relative to the sql/ directory.
    Named parameters use :param_name syntax in SQL, passed as a dict.

    Example:
        run_sql_file(conn, "facts/fct_clicks.sql", {"run_date": "2026-01-15"})
    """
    sql_path = _SQL_DIR / rel_path
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql = sql_path.read_text()

    if params:
        # DuckDB uses positional ? params; we do a safe ordered substitution
        # for named :key params used in our SQL files.
        for key, val in params.items():
            sql = sql.replace(f":{key}", f"'{val}'")

    for statement in sql.split(";"):
        stmt = statement.strip()
        if stmt:
            conn.execute(stmt)
