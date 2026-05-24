"""
tests/fixtures/conftest.py — Shared pytest fixtures for all test layers.
"""
import pytest
import duckdb
from pathlib import Path

SCHEMA_PATH = Path(__file__).parent.parent.parent / "schema.sql"


@pytest.fixture
def conn():
    """In-memory DuckDB with schema bootstrapped. Used by all tests."""
    c = duckdb.connect(":memory:")
    c.execute(SCHEMA_PATH.read_text())
    yield c
    c.close()


@pytest.fixture
def seeded_conn(conn):
    """Connection with campaigns + clicks pre-seeded."""
    conn.execute("""
        INSERT INTO raw_campaigns VALUES
        ('camp-aaa', 'GB', 'operator_A', 'service_1', 'subscription', 'partner-111', 'active', now()),
        ('camp-bbb', 'GB', 'operator_B', 'service_2', 'subscription', 'partner-222', 'active', now()),
        ('camp-ccc', 'GB', 'operator_C', 'service_3', 'one-off',      'partner-333', 'active', now())
    """)
    conn.execute("""
        INSERT INTO raw_clicks VALUES
        ('rotate-aaa', 'camp-aaa', 'pub_1', '2026-01-15 10:00:00+00'),
        ('rotate-bbb', 'camp-bbb', 'pub_2', '2026-01-15 11:00:00+00'),
        ('rotate-ccc', 'camp-ccc', 'pub_3', '2026-01-15 12:00:00+00')
    """)
    return conn
