"""
tests/test_pipeline.py — Smoke tests for the AdStart ETL pipeline.

Run from project root:
    pytest tests/ -v
    pytest tests/ -v -k "attribution"   # run specific tests

Tests use in-memory DuckDB (no CSV files needed) to verify:
  - Schema creation is idempotent
  - dim_campaigns builds correctly
  - Operator A subscription attribution (direct rotate_id)
  - Operator B REN attribution chain (REN → msisdn → SUB → rotate_id → campaign)
  - Out-of-order events (bill before subscribe) are handled
  - Operator C unattributed rows are logged, not crashed
  - Idempotency: re-running same date produces same row count

Bug fix vs original:
  - Path resolution: use sys.path so imports work from both tests/ and root
"""
import sys
from pathlib import Path

# Add project root to sys.path so imports work regardless of where pytest is run from
sys.path.insert(0, str(Path(__file__).parent))

import pytest
from datetime import date
import duckdb


# ── Fixtures ────────────────────────────────────────────────────

@pytest.fixture
def conn():
    """In-memory DuckDB with full schema loaded."""
    c = duckdb.connect(":memory:")
    schema_path = Path(__file__).parent / "schema.sql"
    c.execute(schema_path.read_text())
    yield c
    c.close()


def _seed_campaigns(conn):
    conn.execute("""
        INSERT INTO raw_campaigns VALUES
        ('camp-aaa', 'GB', 'operator_A', 'service_1', 'subscription', 'partner-111', 'active', now()),
        ('camp-bbb', 'GB', 'operator_B', 'service_2', 'subscription', 'partner-222', 'active', now()),
        ('camp-ccc', 'GB', 'operator_C', 'service_3', 'one-off',      'partner-333', 'active', now())
    """)


def _seed_clicks(conn):
    conn.execute("""
        INSERT INTO raw_clicks VALUES
        ('rotate-aaa', 'camp-aaa', 'pub_1', '2026-01-15 10:00:00+00'),
        ('rotate-bbb', 'camp-bbb', 'pub_2', '2026-01-15 11:00:00+00'),
        ('rotate-ccc', 'camp-ccc', 'pub_3', '2026-01-15 12:00:00+00')
    """)


# ── Schema tests ─────────────────────────────────────────────────

def test_schema_is_idempotent(conn):
    """Running schema.sql twice must not raise errors."""
    schema_path = Path(__file__).parent / "schema.sql"
    conn.execute(schema_path.read_text())  # second application
    tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    assert "dim_campaigns" in tables
    assert "fct_subscriptions" in tables
    assert "fct_billing" in tables
    assert "mart_daily_performance" in tables


def test_all_expected_tables_exist(conn):
    tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    expected = {
        "pipeline_runs", "raw_operator_a", "raw_operator_b", "raw_operator_c",
        "raw_campaigns", "raw_clicks", "raw_tracking_codes", "raw_page_events",
        "dim_campaigns", "fct_subscriptions", "fct_billing", "fct_clicks",
        "mart_daily_performance",
    }
    assert expected.issubset(tables), f"Missing tables: {expected - tables}"


# ── dim_campaigns ────────────────────────────────────────────────

def test_dim_campaigns_loads(conn):
    from transform import build_dim_campaigns
    _seed_campaigns(conn)
    count = build_dim_campaigns(conn)
    assert count == 3


def test_dim_campaigns_idempotent(conn):
    """INSERT OR IGNORE — running twice must not duplicate rows."""
    from transform import build_dim_campaigns
    _seed_campaigns(conn)
    build_dim_campaigns(conn)
    build_dim_campaigns(conn)
    count = conn.execute("SELECT COUNT(*) FROM dim_campaigns").fetchone()[0]
    assert count == 3


# ── fct_subscriptions ────────────────────────────────────────────

def test_operator_a_subscription_attribution(conn):
    """Operator A: direct rotate_id → campaign attribution."""
    from transform import build_dim_campaigns, build_fct_subscriptions
    run_date = date(2026, 1, 15)

    _seed_campaigns(conn)
    _seed_clicks(conn)
    build_dim_campaigns(conn)

    # Uses event_time column (fixed from original a.timestamp bug)
    conn.execute(f"""
        INSERT INTO raw_operator_a VALUES
        ('txn-001', 'rotate-aaa', '447700000001', 1, 'SUCCESS', 0.00, 'GBP',
         '2026-01-15 10:05:00+00', '{run_date}')
    """)

    count = build_fct_subscriptions(conn, run_date)
    assert count >= 1

    row = conn.execute("""
        SELECT attribution_method, campaign_id FROM fct_subscriptions
        WHERE operator = 'operator_A'
    """).fetchone()
    assert row is not None
    assert row[0] == "direct_rotate_id"
    assert row[1] == "camp-aaa"


def test_operator_b_ren_attribution_via_msisdn(conn):
    """
    Operator B attribution chain — key insight from Part 1 analysis:
    REN row has no rotate_id → chain: REN.msisdn → SUB.msisdn → rotate_id → campaign.
    """
    from transform import build_dim_campaigns, build_fct_subscriptions, build_fct_billing
    run_date = date(2026, 1, 22)

    _seed_campaigns(conn)
    _seed_clicks(conn)
    build_dim_campaigns(conn)

    # SUB on Jan 15 — has rotate_id
    conn.execute("""
        INSERT INTO raw_operator_b VALUES
        ('txn-sub-001', 'rotate-bbb', '447700000002', 'SUB', 0.00, 'GBP',
         '2026-01-15 11:05:00+00', '2026-01-15')
    """)
    build_fct_subscriptions(conn, date(2026, 1, 15))

    # REN 7 days later — rotate_id intentionally NULL
    conn.execute("""
        INSERT INTO raw_operator_b VALUES
        ('txn-ren-001', NULL, '447700000002', 'REN', 1.99, 'GBP',
         '2026-01-22 11:05:00+00', '2026-01-22')
    """)
    build_fct_subscriptions(conn, run_date)
    ren_count = build_fct_billing(conn, run_date)

    assert ren_count >= 1, "REN billing should be inserted via msisdn chain"

    row = conn.execute("""
        SELECT b.campaign_id FROM fct_billing b
        WHERE b.operator = 'operator_B'
          AND b.billed_at::DATE = '2026-01-22'
    """).fetchone()
    assert row is not None, "REN billing should be attributed to camp-bbb via msisdn"
    assert row[0] == "camp-bbb"


def test_operator_c_bad_tracking_code_does_not_crash(conn):
    """
    Operator C: codes > 3 chars fail the JOIN but must NOT crash the pipeline.
    Only the valid 3-char code row should be inserted.
    (Replicates ~13% unattributable rows from real data analysis in Part 1.)
    """
    from transform import build_dim_campaigns, build_fct_subscriptions
    run_date = date(2026, 1, 15)

    _seed_campaigns(conn)
    _seed_clicks(conn)
    build_dim_campaigns(conn)

    conn.execute("""
        INSERT INTO raw_tracking_codes VALUES
        ('rotate-ccc', 'XYZ', 'svc_1',
         '2026-01-15 12:00:00+00', '2026-01-15 12:30:00+00')
    """)

    conn.execute(f"""
        INSERT INTO raw_operator_c VALUES
        ('msg-001', 'XYZ',  '447700000003', 'DELIVERED', 'svc_1',
         '2026-01-15 12:10:00+00', '{run_date}'),
        ('msg-002', 'XYZW', '447700000004', 'DELIVERED', 'svc_1',
         '2026-01-15 12:15:00+00', '{run_date}')
    """)

    count = build_fct_subscriptions(conn, run_date)
    # Only msg-001 (valid 3-char code) should be attributed
    assert count == 1, f"Expected 1 attributed row, got {count}"


def test_operator_a_bill_before_subscribe_handled(conn):
    """
    Race condition from Part 1.6: bill event arrives up to 120s before subscribe.
    Pipeline must not crash — both rows should be inserted.
    """
    from transform import build_dim_campaigns, build_fct_subscriptions, build_fct_billing
    run_date = date(2026, 1, 15)

    _seed_campaigns(conn)
    _seed_clicks(conn)
    build_dim_campaigns(conn)

    conn.execute(f"""
        INSERT INTO raw_operator_a VALUES
        -- bill arrives 7 seconds BEFORE subscribe (race condition)
        ('txn-bill', 'rotate-aaa', '447700000001', 2, 'SUCCESS', 2.99, 'GBP',
         '2026-01-15 10:04:47+00', '{run_date}'),
        ('txn-sub',  'rotate-aaa', '447700000001', 1, 'SUCCESS', 0.00, 'GBP',
         '2026-01-15 10:04:54+00', '{run_date}')
    """)

    sub_count  = build_fct_subscriptions(conn, run_date)
    bill_count = build_fct_billing(conn, run_date)

    assert sub_count  >= 1, "Subscribe event should be inserted"
    assert bill_count >= 1, "Bill event before subscribe should not crash pipeline"


def test_idempotency_rerun_same_date(conn):
    """Running the full transform twice for the same date must not duplicate rows."""
    from transform import build_dim_campaigns, build_fct_subscriptions
    run_date = date(2026, 1, 15)

    _seed_campaigns(conn)
    _seed_clicks(conn)
    build_dim_campaigns(conn)

    conn.execute(f"""
        INSERT INTO raw_operator_a VALUES
        ('txn-001', 'rotate-aaa', '447700000001', 1, 'SUCCESS', 0.00, 'GBP',
         '2026-01-15 10:05:00+00', '{run_date}')
    """)

    build_fct_subscriptions(conn, run_date)
    first = conn.execute(
        f"SELECT COUNT(*) FROM fct_subscriptions WHERE report_date = '{run_date}'"
    ).fetchone()[0]

    build_fct_subscriptions(conn, run_date)   # second run
    second = conn.execute(
        f"SELECT COUNT(*) FROM fct_subscriptions WHERE report_date = '{run_date}'"
    ).fetchone()[0]

    assert first == second, f"Idempotency violated: {first} → {second} after re-run"


def test_mart_has_no_negative_revenue(conn):
    """Quality gate: mart must not produce negative revenue rows."""
    from transform import (build_dim_campaigns, build_fct_subscriptions,
                           build_fct_billing, build_fct_clicks, build_mart)
    run_date = date(2026, 1, 15)

    _seed_campaigns(conn)
    _seed_clicks(conn)
    build_dim_campaigns(conn)

    conn.execute(f"""
        INSERT INTO raw_operator_a VALUES
        ('txn-sub',  'rotate-aaa', '447700000001', 1, 'SUCCESS', 0.00, 'GBP',
         '2026-01-15 10:05:00+00', '{run_date}'),
        ('txn-bill', 'rotate-aaa', '447700000001', 2, 'SUCCESS', 2.99, 'GBP',
         '2026-01-15 10:15:00+00', '{run_date}')
    """)

    build_fct_subscriptions(conn, run_date)
    build_fct_billing(conn, run_date)
    build_fct_clicks(conn, run_date)
    build_mart(conn, run_date)

    neg = conn.execute(
        f"SELECT COUNT(*) FROM mart_daily_performance "
        f"WHERE report_date = '{run_date}' AND total_revenue < 0"
    ).fetchone()[0]
    assert neg == 0, f"Mart has {neg} rows with negative revenue"