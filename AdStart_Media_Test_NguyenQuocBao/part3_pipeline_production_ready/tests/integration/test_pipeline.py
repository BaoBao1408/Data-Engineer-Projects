"""
tests/integration/test_pipeline.py — Integration tests covering the full ETL pipeline.

These tests use in-memory DuckDB (no files needed) and verify:
  - Operator A direct attribution
  - Operator B REN → msisdn → SUB → rotate_id attribution chain
  - Operator C bad tracking codes are handled (not crashed)
  - Full-run idempotency for same date
"""
import pytest
from datetime import date
import duckdb
from pathlib import Path

SCHEMA_PATH = Path(__file__).parent.parent.parent / "schema.sql"


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    c.execute(SCHEMA_PATH.read_text())
    yield c
    c.close()


def _seed_campaigns(conn):
    conn.execute("""
        INSERT INTO raw_campaigns VALUES
        ('camp-aaa','GB','operator_A','service_1','subscription','partner-111','active',now()),
        ('camp-bbb','GB','operator_B','service_2','subscription','partner-222','active',now()),
        ('camp-ccc','GB','operator_C','service_3','one-off','partner-333','active',now())
    """)


def _seed_clicks(conn):
    conn.execute("""
        INSERT INTO raw_clicks VALUES
        ('rotate-aaa','camp-aaa','pub_1','2026-01-15 10:00:00+00'),
        ('rotate-bbb','camp-bbb','pub_2','2026-01-15 11:00:00+00'),
        ('rotate-ccc','camp-ccc','pub_3','2026-01-15 12:00:00+00')
    """)


def _setup(conn):
    from src.transformations.dimensions import build_dim_campaigns
    _seed_campaigns(conn)
    _seed_clicks(conn)
    build_dim_campaigns(conn)


class TestOperatorAAttribution:
    def test_direct_rotate_id(self, conn):
        """Operator A: event_code=1 with rotate_id → direct_rotate_id attribution."""
        from src.transformations.subscriptions import build_fct_subscriptions
        run_date = date(2026, 1, 15)
        _setup(conn)
        conn.execute(f"""
            INSERT INTO raw_operator_a VALUES
            ('txn-001','rotate-aaa','447700000001',1,'SUCCESS',0.00,'GBP',
             '2026-01-15 10:05:00+00','{run_date}')
        """)
        count = build_fct_subscriptions(conn, run_date)
        assert count >= 1

        row = conn.execute("""
            SELECT attribution_method, campaign_id FROM fct_subscriptions
            WHERE operator = 'operator_A'
        """).fetchone()
        assert row[0] == "direct_rotate_id"
        assert row[1] == "camp-aaa"


class TestOperatorBRenAttribution:
    def test_ren_chains_via_msisdn(self, conn):
        """
        Operator B REN rows have no rotate_id.
        Chain: REN.msisdn → most-recent SUB → rotate_id → campaign.
        """
        from src.transformations.subscriptions import build_fct_subscriptions
        from src.transformations.billing_clicks_mart import build_fct_billing

        _setup(conn)

        # SUB on Jan 15 (has rotate_id)
        conn.execute("""
            INSERT INTO raw_operator_b VALUES
            ('txn-sub-001','rotate-bbb','447700000002','SUB',0.00,'GBP',
             '2026-01-15 11:05:00+00','2026-01-15')
        """)
        build_fct_subscriptions(conn, date(2026, 1, 15))

        # REN on Jan 22 (no rotate_id — renewal 7 days later)
        conn.execute("""
            INSERT INTO raw_operator_b VALUES
            ('txn-ren-001',NULL,'447700000002','REN',1.99,'GBP',
             '2026-01-22 11:05:00+00','2026-01-22')
        """)
        build_fct_subscriptions(conn, date(2026, 1, 22))
        ren_count = build_fct_billing(conn, date(2026, 1, 22))

        assert ren_count >= 1, "REN billing row should be inserted via msisdn attribution"
        row = conn.execute("""
            SELECT campaign_id FROM fct_billing
            WHERE operator = 'operator_B' AND billed_at::DATE = '2026-01-22'
        """).fetchone()
        assert row is not None, "REN billing must be attributed to camp-bbb"
        assert row[0] == "camp-bbb"


class TestOperatorCTracking:
    def test_bad_tracking_code_does_not_crash(self, conn):
        """
        Operator C: tracking_codes > 3 chars cannot join lookup table.
        Pipeline must log and continue, not raise.
        Only the valid 3-char code row should be attributed.
        """
        from src.transformations.subscriptions import build_fct_subscriptions
        run_date = date(2026, 1, 15)
        _setup(conn)

        conn.execute("""
            INSERT INTO raw_tracking_codes VALUES
            ('rotate-ccc','XYZ','svc_1','2026-01-15 12:00:00+00','2026-01-15 12:30:00+00')
        """)
        conn.execute(f"""
            INSERT INTO raw_operator_c VALUES
            ('msg-001','XYZ','447700000003','DELIVERED','svc_1','2026-01-15 12:10:00+00','{run_date}'),
            ('msg-002','XYZW','447700000004','DELIVERED','svc_1','2026-01-15 12:15:00+00','{run_date}')
        """)

        count = build_fct_subscriptions(conn, run_date)
        assert count == 1, "Only the valid 3-char tracking code row should be attributed"


class TestIdempotency:
    def test_rerun_same_date_no_duplicates(self, conn):
        """Running pipeline twice for the same date must not duplicate rows."""
        from src.transformations.subscriptions import build_fct_subscriptions
        run_date = date(2026, 1, 15)
        _setup(conn)

        conn.execute(f"""
            INSERT INTO raw_operator_a VALUES
            ('txn-001','rotate-aaa','447700000001',1,'SUCCESS',0.00,'GBP',
             '2026-01-15 10:05:00+00','{run_date}')
        """)

        build_fct_subscriptions(conn, run_date)
        count_first = conn.execute(
            f"SELECT COUNT(*) FROM fct_subscriptions WHERE report_date = '{run_date}'"
        ).fetchone()[0]

        build_fct_subscriptions(conn, run_date)
        count_second = conn.execute(
            f"SELECT COUNT(*) FROM fct_subscriptions WHERE report_date = '{run_date}'"
        ).fetchone()[0]

        assert count_first == count_second, "Idempotency violated: re-run produced different count"
