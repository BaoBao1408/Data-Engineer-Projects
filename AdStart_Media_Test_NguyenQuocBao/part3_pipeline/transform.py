"""
transform.py — Populate fact tables and mart from staging tables.

Layer order:
  1. dim_campaigns          — SCD Type 1, INSERT OR IGNORE
  2. fct_subscriptions      — attribution resolution per operator
  3. fct_billing            — is_first_bill + billing_sequence via window fn
  4. fct_clicks             — conversion funnel flags per click
  5. mart_daily_performance — pre-aggregated daily rollup

Fixes vs original skeleton:
  - Op A subscriptions : a.timestamp  →  a.event_time   (staging col name)
  - Op A billing       : a.timestamp  →  a.event_time   (4 occurrences)
  - Op C subscriptions : LEFT JOIN    →  INNER JOIN
                         + LENGTH(tracking_code) = 3 guard
                         so invalid codes never enter fct_subscriptions
"""
import logging
from datetime import date

import duckdb

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 1. dim_campaigns
# ─────────────────────────────────────────────────────────────
def build_dim_campaigns(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Populate dim_campaigns from raw_campaigns.
    INSERT OR IGNORE → existing rows untouched (SCD Type 1).
    AWS: Glue upsert into Redshift, or dbt snapshot for SCD Type 2.
    """
    conn.execute("""
        INSERT OR IGNORE INTO dim_campaigns
        SELECT
            id            AS campaign_id,
            operator,
            service_name,
            service_model,
            partner_id,
            status,
            created_at,
            now()         AS loaded_at
        FROM raw_campaigns
        WHERE id IS NOT NULL
    """)
    count = conn.execute("SELECT COUNT(*) FROM dim_campaigns").fetchone()[0]
    logger.info(f"[dim_campaigns] {count:,} rows total.")
    return count


# ─────────────────────────────────────────────────────────────
# 2. fct_subscriptions
# ─────────────────────────────────────────────────────────────
def build_fct_subscriptions(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Merge subscription events from all 3 operators.

    Attribution:
      operator_A — direct rotate_id → campaign_id
      operator_B — direct rotate_id (SUB rows only;
                   REN/UNSUB have no rotate_id → handled in fct_billing)
      operator_C — tracking_code (exactly 3 chars, within 30-min window)
                   → rotate_id → campaign_id
                   Rows that cannot resolve are logged + skipped.

    IDEMPOTENCY: DELETE report_date before INSERT.
    """
    conn.execute(f"DELETE FROM fct_subscriptions WHERE report_date = '{run_date}'")

    # ── Operator A: event_code = 1 ────────────────────────────
    conn.execute(f"""
        INSERT INTO fct_subscriptions
        SELECT
            gen_random_uuid()          AS subscription_id,
            'operator_A'               AS operator,
            a.transaction_id           AS source_transaction_id,
            a.rotate_id,
            c.campaign_id,
            c.service_name,
            c.partner_id,
            a.msisdn,
            a.event_time               AS subscribed_at,
            a.event_time::DATE         AS report_date,
            'direct_rotate_id'         AS attribution_method,
            now()                      AS loaded_at
        FROM raw_operator_a a
        JOIN raw_clicks cl   ON cl.rotate_id  = a.rotate_id
        JOIN dim_campaigns c ON c.campaign_id = cl.campaign_id
        WHERE a.event_code   = 1
          AND a._loaded_date = '{run_date}'
          AND a.rotate_id IS NOT NULL
    """)

    # ── Operator B: transaction_type = 'SUB' ──────────────────
    conn.execute(f"""
        INSERT INTO fct_subscriptions
        SELECT
            gen_random_uuid()          AS subscription_id,
            'operator_B'               AS operator,
            b.transaction_id           AS source_transaction_id,
            b.rotate_id,
            c.campaign_id,
            c.service_name,
            c.partner_id,
            b.msisdn,
            b.created_at               AS subscribed_at,
            b.created_at::DATE         AS report_date,
            'direct_rotate_id'         AS attribution_method,
            now()                      AS loaded_at
        FROM raw_operator_b b
        JOIN raw_clicks cl   ON cl.rotate_id  = b.rotate_id
        JOIN dim_campaigns c ON c.campaign_id = cl.campaign_id
        WHERE b.transaction_type = 'SUB'
          AND b._loaded_date     = '{run_date}'
          AND b.rotate_id IS NOT NULL
    """)

    # ── Operator C: delivery_status = 'DELIVERED' ─────────────
    # INNER JOIN: only rows where code resolves to a rotate_id.
    # LENGTH = 3 guard: drops ~13% of rows with SMS-parser suffix before the join.
    conn.execute(f"""
        INSERT INTO fct_subscriptions
        SELECT
            gen_random_uuid()          AS subscription_id,
            'operator_C'               AS operator,
            oc.message_id              AS source_transaction_id,
            tc.rotate_id,
            c.campaign_id,
            c.service_name,
            c.partner_id,
            oc.msisdn,
            oc.received_time           AS subscribed_at,
            oc.received_time::DATE     AS report_date,
            'tracking_code_lookup'     AS attribution_method,
            now()                      AS loaded_at
        FROM raw_operator_c oc
        JOIN raw_tracking_codes tc
            ON  tc.code                  = oc.tracking_code
            AND LENGTH(oc.tracking_code) = 3
            AND oc.received_time BETWEEN tc.created_at AND tc.expired_at
        JOIN raw_clicks cl   ON cl.rotate_id  = tc.rotate_id
        JOIN dim_campaigns c ON c.campaign_id = cl.campaign_id
        WHERE oc.delivery_status = 'DELIVERED'
          AND oc._loaded_date    = '{run_date}'
    """)

    # Log unattributed op-C rows (revenue leakage visibility)
    unattr = conn.execute(f"""
        SELECT COUNT(*)
        FROM raw_operator_c oc
        LEFT JOIN raw_tracking_codes tc
            ON  tc.code                  = oc.tracking_code
            AND LENGTH(oc.tracking_code) = 3
            AND oc.received_time BETWEEN tc.created_at AND tc.expired_at
        WHERE oc.delivery_status = 'DELIVERED'
          AND oc._loaded_date    = '{run_date}'
          AND tc.rotate_id IS NULL
    """).fetchone()[0]
    if unattr > 0:
        logger.warning(
            f"[fct_subscriptions] {unattr} operator_C DELIVERED rows unattributed "
            f"(tracking_code >3 chars or expired). Expected ~13% — known SMS parser issue."
        )

    count = conn.execute(
        f"SELECT COUNT(*) FROM fct_subscriptions WHERE report_date = '{run_date}'"
    ).fetchone()[0]
    logger.info(f"[fct_subscriptions] {count:,} rows for {run_date}.")
    return count


# ─────────────────────────────────────────────────────────────
# 3. fct_billing
# ─────────────────────────────────────────────────────────────
def build_fct_billing(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Merge billing events from all 3 operators.

    operator_A — event_code=2, status=SUCCESS; rotate_id always present
    operator_B — transaction_type=REN, amount>0; REN rows have NO rotate_id
                 Attribution: REN.msisdn → most recent SUB before this REN → campaign
    operator_C — DELIVERED = subscribe + first charge simultaneously; amount=0.00

    is_first_bill / billing_sequence via ROW_NUMBER per (msisdn, campaign_id).

    IDEMPOTENCY: DELETE report_date before INSERT.
    """
    conn.execute(f"DELETE FROM fct_billing WHERE report_date = '{run_date}'")

    # ── Operator A ────────────────────────────────────────────
    conn.execute(f"""
        INSERT INTO fct_billing
        SELECT
            gen_random_uuid()           AS billing_id,
            'operator_A'                AS operator,
            a.transaction_id            AS source_transaction_id,
            sub.subscription_id,
            c.campaign_id,
            c.service_name,
            c.partner_id,
            a.msisdn,
            COALESCE(a.amount, 0)       AS amount,
            COALESCE(a.currency, 'GBP') AS currency,
            a.event_time                AS billed_at,
            a.event_time::DATE          AS report_date,
            (ROW_NUMBER() OVER (
                PARTITION BY a.msisdn, cl.campaign_id
                ORDER BY a.event_time
            ) = 1)                      AS is_first_bill,
            ROW_NUMBER() OVER (
                PARTITION BY a.msisdn, cl.campaign_id
                ORDER BY a.event_time
            )                           AS billing_sequence,
            a.status                    AS billing_status,
            now()                       AS loaded_at
        FROM raw_operator_a a
        JOIN raw_clicks cl   ON cl.rotate_id  = a.rotate_id
        JOIN dim_campaigns c ON c.campaign_id = cl.campaign_id
        LEFT JOIN fct_subscriptions sub
            ON  sub.msisdn        = a.msisdn
            AND sub.operator      = 'operator_A'
            AND sub.subscribed_at = (
                SELECT MAX(s2.subscribed_at)
                FROM fct_subscriptions s2
                WHERE s2.msisdn       = a.msisdn
                  AND s2.operator     = 'operator_A'
                  AND s2.subscribed_at <= a.event_time
            )
        WHERE a.event_code   = 2
          AND UPPER(COALESCE(a.status, '')) = 'SUCCESS'
          AND a._loaded_date = '{run_date}'
    """)

    # ── Operator B renewals ───────────────────────────────────
    conn.execute(f"""
        INSERT INTO fct_billing
        SELECT
            gen_random_uuid()           AS billing_id,
            'operator_B'                AS operator,
            b.transaction_id            AS source_transaction_id,
            sub.subscription_id,
            c.campaign_id,
            c.service_name,
            c.partner_id,
            b.msisdn,
            COALESCE(b.amount, 0)       AS amount,
            COALESCE(b.currency, 'GBP') AS currency,
            b.created_at                AS billed_at,
            b.created_at::DATE          AS report_date,
            (ROW_NUMBER() OVER (
                PARTITION BY b.msisdn, sub.campaign_id
                ORDER BY b.created_at
            ) = 1)                      AS is_first_bill,
            ROW_NUMBER() OVER (
                PARTITION BY b.msisdn, sub.campaign_id
                ORDER BY b.created_at
            )                           AS billing_sequence,
            'SUCCESS'                   AS billing_status,
            now()                       AS loaded_at
        FROM raw_operator_b b
        LEFT JOIN fct_subscriptions sub
            ON  sub.msisdn        = b.msisdn
            AND sub.operator      = 'operator_B'
            AND sub.subscribed_at = (
                SELECT MAX(s2.subscribed_at)
                FROM fct_subscriptions s2
                WHERE s2.msisdn       = b.msisdn
                  AND s2.operator     = 'operator_B'
                  AND s2.subscribed_at <= b.created_at
            )
        LEFT JOIN dim_campaigns c ON c.campaign_id = sub.campaign_id
        WHERE b.transaction_type = 'REN'
          AND COALESCE(b.amount, 0) > 0
          AND b._loaded_date    = '{run_date}'
          AND c.campaign_id IS NOT NULL
    """)

    # ── Operator C ────────────────────────────────────────────
    conn.execute(f"""
        INSERT INTO fct_billing
        SELECT
            gen_random_uuid()           AS billing_id,
            'operator_C'                AS operator,
            oc.message_id               AS source_transaction_id,
            sub.subscription_id,
            sub.campaign_id,
            sub.service_name,
            sub.partner_id,
            oc.msisdn,
            0.00                        AS amount,
            'GBP'                       AS currency,
            oc.received_time            AS billed_at,
            oc.received_time::DATE      AS report_date,
            TRUE                        AS is_first_bill,
            1                           AS billing_sequence,
            'SUCCESS'                   AS billing_status,
            now()                       AS loaded_at
        FROM raw_operator_c oc
        JOIN fct_subscriptions sub
            ON  sub.source_transaction_id = oc.message_id
            AND sub.operator              = 'operator_C'
        WHERE oc.delivery_status = 'DELIVERED'
          AND oc._loaded_date    = '{run_date}'
    """)

    count = conn.execute(
        f"SELECT COUNT(*) FROM fct_billing WHERE report_date = '{run_date}'"
    ).fetchone()[0]
    logger.info(f"[fct_billing] {count:,} rows for {run_date}.")
    return count


# ─────────────────────────────────────────────────────────────
# 4. fct_clicks
# ─────────────────────────────────────────────────────────────
def build_fct_clicks(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Enrich every click with Boolean conversion-funnel flags.
    Pre-computing here means mart query is a cheap SUM(), not a re-join.

    IDEMPOTENCY: DELETE + INSERT for run_date.
    """
    conn.execute(f"DELETE FROM fct_clicks WHERE report_date = '{run_date}'")

    conn.execute(f"""
        INSERT INTO fct_clicks
        SELECT
            cl.rotate_id,
            cl.campaign_id,
            c.service_name,
            c.operator,
            c.partner_id,
            cl.pub_id,
            cl.clicked_at,
            cl.clicked_at::DATE                    AS report_date,

            COALESCE(pv.has_page_view,  FALSE)     AS has_page_view,
            COALESCE(pv.has_cta_click,  FALSE)     AS has_cta_click,
            COALESCE(pv.has_entry,      FALSE)     AS has_entry,
            COALESCE(sub.has_sub,       FALSE)     AS has_subscription,
            COALESCE(bill.has_bill,     FALSE)     AS has_first_bill,

            now()                                  AS loaded_at
        FROM raw_clicks cl
        JOIN dim_campaigns c ON c.campaign_id = cl.campaign_id

        LEFT JOIN (
            SELECT
                rotate_id,
                BOOL_OR(event_type = 'VIEW')      AS has_page_view,
                BOOL_OR(event_type = 'CLICK_CTA') AS has_cta_click,
                BOOL_OR(event_type = 'ENTRY')     AS has_entry
            FROM raw_page_events
            GROUP BY rotate_id
        ) pv ON pv.rotate_id = cl.rotate_id

        LEFT JOIN (
            SELECT rotate_id, TRUE AS has_sub
            FROM fct_subscriptions
            WHERE rotate_id IS NOT NULL
            GROUP BY rotate_id
        ) sub ON sub.rotate_id = cl.rotate_id

        LEFT JOIN (
            SELECT fs.rotate_id, TRUE AS has_bill
            FROM fct_billing fb
            JOIN fct_subscriptions fs ON fs.subscription_id = fb.subscription_id
            WHERE fb.is_first_bill = TRUE
              AND fs.rotate_id IS NOT NULL
            GROUP BY fs.rotate_id
        ) bill ON bill.rotate_id = cl.rotate_id

        WHERE cl.clicked_at::DATE = '{run_date}'
    """)

    count = conn.execute(
        f"SELECT COUNT(*) FROM fct_clicks WHERE report_date = '{run_date}'"
    ).fetchone()[0]
    logger.info(f"[fct_clicks] {count:,} rows for {run_date}.")
    return count


# ─────────────────────────────────────────────────────────────
# 5. mart_daily_performance
# ─────────────────────────────────────────────────────────────
def build_mart(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Pre-aggregate all metrics into mart_daily_performance.
    Grain: 1 row per (report_date, campaign_id).
    BI tools query this table directly — never the raw/fact layers.

    IDEMPOTENCY: DELETE WHERE report_date = run_date before insert.
    AWS: dbt model on Athena, or Redshift materialized view refresh.
    """
    conn.execute(f"DELETE FROM mart_daily_performance WHERE report_date = '{run_date}'")

    conn.execute(f"""
        INSERT INTO mart_daily_performance
        SELECT
            cl.report_date,
            cl.campaign_id,
            cl.operator,
            cl.service_name,
            cl.partner_id,

            COUNT(*)                                        AS total_clicks,
            SUM(cl.has_page_view::INTEGER)                  AS total_page_views,
            SUM(cl.has_cta_click::INTEGER)                  AS total_cta_clicks,
            SUM(cl.has_entry::INTEGER)                      AS total_entries,
            SUM(cl.has_subscription::INTEGER)               AS total_subscriptions,
            SUM(cl.has_first_bill::INTEGER)                 AS total_first_bills,

            COALESCE((
                SELECT COUNT(*)
                FROM fct_billing fb
                WHERE fb.report_date   = '{run_date}'
                  AND fb.campaign_id   = cl.campaign_id
                  AND fb.is_first_bill = FALSE
            ), 0)                                           AS total_renewals,

            COALESCE((
                SELECT SUM(fb.amount)
                FROM fct_billing fb
                WHERE fb.report_date    = '{run_date}'
                  AND fb.campaign_id    = cl.campaign_id
                  AND fb.billing_status = 'SUCCESS'
            ), 0)                                           AS total_revenue,

            'GBP'                                           AS currency,

            ROUND(
                SUM(cl.has_subscription::INTEGER)::DECIMAL
                / NULLIF(COUNT(*), 0), 6
            )                                               AS sub_conversion_rate,
            ROUND(
                SUM(cl.has_first_bill::INTEGER)::DECIMAL
                / NULLIF(COUNT(*), 0), 6
            )                                               AS bill_conversion_rate,

            now()                                           AS loaded_at

        FROM fct_clicks cl
        WHERE cl.report_date = '{run_date}'
        GROUP BY 1, 2, 3, 4, 5
    """)

    count = conn.execute(
        f"SELECT COUNT(*) FROM mart_daily_performance WHERE report_date = '{run_date}'"
    ).fetchone()[0]
    logger.info(f"[mart_daily_performance] {count:,} rows for {run_date}.")
    return count