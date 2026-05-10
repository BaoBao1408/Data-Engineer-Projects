"""
tests/unit/test_dimensions.py — Unit tests for schema + dim_campaigns.
"""
import pytest
import duckdb
from pathlib import Path
from datetime import date

SCHEMA_PATH = Path(__file__).parent.parent.parent / "schema.sql"


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    c.execute(SCHEMA_PATH.read_text())
    yield c
    c.close()


def _seed(conn):
    conn.execute("""
        INSERT INTO raw_campaigns VALUES
        ('camp-aaa','GB','operator_A','service_1','subscription','partner-111','active',now()),
        ('camp-bbb','GB','operator_B','service_2','subscription','partner-222','active',now()),
        ('camp-ccc','GB','operator_C','service_3','one-off','partner-333','active',now())
    """)


class TestSchema:
    def test_is_idempotent(self, conn):
        """Running schema.sql twice must not raise."""
        conn.execute(SCHEMA_PATH.read_text())
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        assert "dim_campaigns" in tables
        assert "fct_subscriptions" in tables
        assert "fct_billing" in tables
        assert "mart_daily_performance" in tables

    def test_all_expected_tables_exist(self, conn):
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        expected = {
            "pipeline_runs", "raw_operator_a", "raw_operator_b", "raw_operator_c",
            "raw_campaigns", "raw_clicks", "raw_tracking_codes", "raw_page_events",
            "dim_campaigns", "fct_subscriptions", "fct_billing", "fct_clicks",
            "mart_daily_performance",
        }
        missing = expected - tables
        assert not missing, f"Missing tables: {missing}"


class TestDimCampaigns:
    def test_loads_correctly(self, conn):
        from temp.src.transformations.dimensions import build_dim_campaigns
        _seed(conn)
        count = build_dim_campaigns(conn)
        assert count == 3

    def test_is_idempotent(self, conn):
        """INSERT OR IGNORE — running twice must not duplicate rows."""
        from temp.src.transformations.dimensions import build_dim_campaigns
        _seed(conn)
        build_dim_campaigns(conn)
        build_dim_campaigns(conn)
        count = conn.execute("SELECT COUNT(*) FROM dim_campaigns").fetchone()[0]
        assert count == 3
