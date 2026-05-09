"""
transform.py — Populate fact tables and mart from staging tables.

Core logic:
  1. dim_campaigns        — deduplicated reference data
  2. fct_subscriptions    — with attribution resolution per operator
  3. fct_billing          — with is_first_bill + billing_sequence
  4. fct_clicks           — with conversion funnel flags
  5. mart_daily_performance — pre-aggregated daily rollup

AWS equivalent: each function here = 1 Glue job or 1 dbt model.
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
    Uses INSERT OR IGNORE to keep existing rows untouched (SCD Type 1 — static attributes).

    AWS: Glue job with upsert into Redshift, or dbt snapshot for SCD Type 2.
    """
    conn.execute("""
        INSERT OR IGNORE INTO dim_campaigns
        SELECT
            id           AS campaign_id,
            operator,
            service_name,
            service_model,
            partner_id,
            status,
            created_at,
            now()        AS loaded_at
        FROM raw_campaigns
        WHERE id IS NOT NULL
    """)
    count = conn.execute("SELECT COUNT(*) FROM dim_campaigns").fetchone()[0]
    logger.info(f"[dim_campaigns] {count:,} rows total.")
    return count


# ─────────────────────────────────────────────────────────────
# 2. fct_subscriptions — Attribution resolution
# ─────────────────────────────────────────────────────────────
def build_fct_subscriptions(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Merge subscription events from all 3 operators.
    Attribution method per operator:
      - operator_a: direct rotate_id → campaign_id
      - operator_b: direct rotate_id → campaign_id
      - operator_c: tracking_code → lookup → rotate_id → campaign_id

    IDEMPOTENCY: delete today's rows before inserting.
    """
    conn.execute(f"DELETE FROM fct_subscriptions WHERE report_date = '{run_date}'")

    # ── Operator A: event_code = 1 ────────────────────────────
    conn.execute(f"""
        INSERT INTO fct_subscriptions
        SELECT
            gen_random_uuid()               AS subscription_id,
            'operator_A'                    AS operator,
            a.transaction_id                AS source_transaction_id,
            a.rotate_id,
            c.campaign_id,
            c.service_name,
            c.partner_id,
            a.msisdn,
            a.timestamp                     AS subscribed_at,
            a.timestamp::DATE               AS report_date,
            'direct_rotate_id'              AS attribution_method,
            now()                           AS loaded_at
        FROM raw_operator_a a
        JOIN raw_clicks cl ON cl.rotate_id = a.rotate_id
        JOIN dim_campaigns c  ON c.campaign_id = cl.campaign_id
        WHERE a.event_code = 1
          AND a._loaded_date = '{run_date}'
          AND a.rotate_id IS NOT NULL
    """)

    # ── Operator B: transaction_type = 'SUB' ──────────────────
    conn.execute(f"""
        INSERT INTO fct_subscriptions
        SELECT
            gen_random_uuid()               AS subscription_id,
            'operator_B'                    AS operator,
            b.transaction_id                AS source_transaction_id,
            b.rotate_id,
            c.campaign_id,
            c.service_name,
            c.partner_id,
            b.msisdn,
            b.created_at                    AS subscribed_at,
            b.created_at::DATE              AS report_date,
            'direct_rotate_id'              AS attribution_method,
            now()                           AS loaded_at
        FROM raw_operator_b b
        JOIN raw_clicks cl ON cl.rotate_id = b.rotate_id
        JOIN dim_campaigns c  ON c.campaign_id = cl.campaign_id
        WHERE b.transaction_type = 'SUB'
          AND b._loaded_date = '{run_date}'
          AND b.rotate_id IS NOT NULL
    """)

    # ── Operator C: delivery_status = 'DELIVERED' ─────────────
    # Step 1: try to resolve tracking_code → rotate_id via tracking_codes table
    # Step 2: if resolved → attribution_method = 'tracking_code_lookup'
    # Step 3: if not resolved → attribution_method = 'unattributed', campaign_id = NULL (skip for now)
    conn.execute(f"""
        INSERT INTO fct_subscriptions
        SELECT
            gen_random_uuid()               AS subscription_id,
            'operator_C'                    AS operator,
            oc.message_id                   AS source_transaction_id,
            tc.rotate_id,
            c.campaign_id,
            c.service_name,
            c.partner_id,
            oc.msisdn,
            oc.received_time                AS subscribed_at,
            oc.received_time::DATE          AS report_date,
            CASE
                WHEN tc.rotate_id IS NOT NULL THEN 'tracking_code_lookup'
                ELSE 'unattributed'
            END                             AS attribution_method,
            now()                           AS loaded_at
        FROM raw_operator_c oc
        -- Lookup: match tracking_code within its validity window
        LEFT JOIN raw_tracking_codes tc
            ON tc.code = oc.tracking_code
           AND oc.received_time BETWEEN tc.created_at AND tc.expired_at
        LEFT JOIN raw_clicks cl ON cl.rotate_id = tc.rotate_id
        LEFT JOIN dim_campaigns c  ON c.campaign_id = cl.campaign_id
        WHERE oc.delivery_status = 'DELIVERED'
          AND oc._loaded_date = '{run_date}'
          -- Only insert rows where we could resolve campaign_id
          -- Unattributed rows are logged separately
          AND c.campaign_id IS NOT NULL
    """)

    # Log unattributed operator C rows (for business visibility)
    unattr_count = conn.execute(f"""
        SELECT COUNT(*) FROM raw_operator_c oc
        LEFT JOIN raw_tracking_codes tc
            ON tc.code = oc.tracking_code
           AND oc.received_time BETWEEN tc.created_at AND tc.expired_at
        WHERE oc.delivery_status = 'DELIVERED'
          AND oc._loaded_date = '{run_date}'
          AND tc.rotate_id IS NULL
    """).fetchone()[0]
    if unattr_count > 0:
        logger.warning(f"[fct_subscriptions] {unattr_count} operator_C rows could not be attributed (expired/missing tracking_code).")

    count = conn.execute(f"SELECT COUNT(*) FROM fct_subscriptions WHERE report_date = '{run_date}'").fetchone()[0]
    logger.info(f"[fct_subscriptions] {count:,} rows inserted for {run_date}.")
    return count


# ─────────────────────────────────────────────────────────────
# 3. fct_billing
# ─────────────────────────────────────────────────────────────
def build_fct_billing(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Merge billing events from all operators.
    - operator_a: event_code = 2, status = 'SUCCESS'
    - operator_b: transaction_type = 'REN', amount > 0
    - operator_c: delivery_status = 'DELIVERED' (combined event, amount = campaign default)

    is_first_bill and billing_sequence are computed via window functions at insert time.
    """
    conn.execute(f"DELETE FROM fct_billing WHERE report_date = '{run_date}'")

    # ── Operator A billings ───────────────────────────────────
    conn.execute(f"""
        INSERT INTO fct_billing
        SELECT
            gen_random_uuid()               AS billing_id,
            'operator_A'                    AS operator,
            a.transaction_id                AS source_transaction_id,
            sub.subscription_id,
            c.campaign_id,
            c.service_name,
            c.partner_id,
            a.msisdn,
            COALESCE(a.amount, 0)           AS amount,
            'GBP'                           AS currency,
            a.timestamp                     AS billed_at,
            a.timestamp::DATE               AS report_date,
            -- is_first_bill: TRUE if this is the first billing event for this msisdn + campaign
            (ROW_NUMBER() OVER (
                PARTITION BY a.msisdn, cl.campaign_id
                ORDER BY a.timestamp
            ) = 1)                          AS is_first_bill,
            ROW_NUMBER() OVER (
                PARTITION BY a.msisdn, cl.campaign_id
                ORDER BY a.timestamp
            )                               AS billing_sequence,
            'SUCCESS'                       AS billing_status,
            now()                           AS loaded_at
        FROM raw_operator_a a
        JOIN raw_clicks cl  ON cl.rotate_id = a.rotate_id
        JOIN dim_campaigns c   ON c.campaign_id = cl.campaign_id
        -- Link back to subscription for this msisdn (most recent SUB before this billing)
        LEFT JOIN fct_subscriptions sub
            ON sub.msisdn = a.msisdn
           AND sub.operator = 'operator_A'
           AND sub.subscribed_at = (
               SELECT MAX(s2.subscribed_at)
               FROM fct_subscriptions s2
               WHERE s2.msisdn = a.msisdn
                 AND s2.operator = 'operator_A'
                 AND s2.subscribed_at <= a.timestamp
           )
        WHERE a.event_code = 2
          AND UPPER(a.status) = 'SUCCESS'
          AND a._loaded_date = '{run_date}'
    """)

    # ── Operator B renewals ───────────────────────────────────
    conn.execute(f"""
        INSERT INTO fct_billing
        SELECT
            gen_random_uuid()               AS billing_id,
            'operator_B'                    AS operator,
            b.transaction_id                AS source_transaction_id,
            sub.subscription_id,
            c.campaign_id,
            c.service_name,
            c.partner_id,
            b.msisdn,
            COALESCE(b.amount, 0)           AS amount,
            'GBP'                           AS currency,
            b.created_at                    AS billed_at,
            b.created_at::DATE              AS report_date,
            (ROW_NUMBER() OVER (
                PARTITION BY b.msisdn, sub.campaign_id
                ORDER BY b.created_at
            ) = 1)                          AS is_first_bill,
            ROW_NUMBER() OVER (
                PARTITION BY b.msisdn, sub.campaign_id
                ORDER BY b.created_at
            )                               AS billing_sequence,
            'SUCCESS'                       AS billing_status,
            now()                           AS loaded_at
        FROM raw_operator_b b
        -- For REN, no rotate_id → link via msisdn to most recent SUB
        LEFT JOIN fct_subscriptions sub
            ON sub.msisdn = b.msisdn
           AND sub.operator = 'operator_B'
           AND sub.subscribed_at = (
               SELECT MAX(s2.subscribed_at)
               FROM fct_subscriptions s2
               WHERE s2.msisdn = b.msisdn
                 AND s2.operator = 'operator_B'
                 AND s2.subscribed_at <= b.created_at
           )
        LEFT JOIN dim_campaigns c ON c.campaign_id = sub.campaign_id
        WHERE b.transaction_type = 'REN'
          AND COALESCE(b.amount, 0) > 0
          AND b._loaded_date = '{run_date}'
          AND c.campaign_id IS NOT NULL
    """)

    # ── Operator C combined events ────────────────────────────
    # delivery_status='DELIVERED' means subscription + first charge happened together
    conn.execute(f"""
        INSERT INTO fct_billing
        SELECT
            gen_random_uuid()               AS billing_id,
            'operator_C'                    AS operator,
            oc.message_id                   AS source_transaction_id,
            sub.subscription_id,
            sub.campaign_id,
            sub.service_name,
            sub.partner_id,
            oc.msisdn,
            0.00                            AS amount,  -- operator_C doesn't report amount
            'GBP'                           AS currency,
            oc.received_time                AS billed_at,
            oc.received_time::DATE          AS report_date,
            TRUE                            AS is_first_bill,  -- always first since sub+bill = same event
            1                               AS billing_sequence,
            'SUCCESS'                       AS billing_status,
            now()                           AS loaded_at
        FROM raw_operator_c oc
        JOIN fct_subscriptions sub
            ON sub.source_transaction_id = oc.message_id
           AND sub.operator = 'operator_C'
        WHERE oc.delivery_status = 'DELIVERED'
          AND oc._loaded_date = '{run_date}'
    """)

    count = conn.execute(f"SELECT COUNT(*) FROM fct_billing WHERE report_date = '{run_date}'").fetchone()[0]
    logger.info(f"[fct_billing] {count:,} rows inserted for {run_date}.")
    return count


# ─────────────────────────────────────────────────────────────
# 4. fct_clicks — Conversion funnel flags
# ─────────────────────────────────────────────────────────────
def build_fct_clicks(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Enrich clicks with pre-computed conversion flags.
    All flags are Boolean: TRUE = user reached that funnel step.

    IDEMPOTENCY: DELETE + INSERT for run_date's clicks.
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
            cl.clicked_at::DATE             AS report_date,

            -- Conversion funnel flags (pre-computed for fast dashboard queries)
            COALESCE(pv.has_page_view,  FALSE) AS has_page_view,
            COALESCE(pv.has_cta_click,  FALSE) AS has_cta_click,
            COALESCE(pv.has_entry,      FALSE) AS has_entry,
            COALESCE(sub.has_sub,       FALSE) AS has_subscription,
            COALESCE(bill.has_bill,     FALSE) AS has_first_bill,

            now()                           AS loaded_at
        FROM raw_clicks cl
        JOIN dim_campaigns c ON c.campaign_id = cl.campaign_id

        -- Page events: pivot VIEW / CLICK_CTA / ENTRY into boolean columns
        LEFT JOIN (
            SELECT
                rotate_id,
                BOOL_OR(event_type = 'VIEW')       AS has_page_view,
                BOOL_OR(event_type = 'CLICK_CTA')  AS has_cta_click,
                BOOL_OR(event_type = 'ENTRY')       AS has_entry
            FROM raw_page_events
            GROUP BY rotate_id
        ) pv ON pv.rotate_id = cl.rotate_id

        -- Subscription flag
        LEFT JOIN (
            SELECT rotate_id, TRUE AS has_sub
            FROM fct_subscriptions
            WHERE rotate_id IS NOT NULL
            GROUP BY rotate_id
        ) sub ON sub.rotate_id = cl.rotate_id

        -- First bill flag
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

    count = conn.execute(f"SELECT COUNT(*) FROM fct_clicks WHERE report_date = '{run_date}'").fetchone()[0]
    logger.info(f"[fct_clicks] {count:,} rows inserted for {run_date}.")
    return count


# ─────────────────────────────────────────────────────────────
# 5. mart_daily_performance
# ─────────────────────────────────────────────────────────────
def build_mart(conn: duckdb.DuckDBPyConnection, run_date: date) -> int:
    """
    Pre-aggregate all metrics into mart_daily_performance.
    This is what BI tools (Metabase, Looker) query directly.

    IDEMPOTENCY: DELETE WHERE report_date = run_date before insert.
    AWS: dbt run --select mart_daily_performance, or Redshift materialized view refresh.
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

            -- Click funnel
            COUNT(*)                                    AS total_clicks,
            SUM(cl.has_page_view::INTEGER)              AS total_page_views,
            SUM(cl.has_cta_click::INTEGER)              AS total_cta_clicks,
            SUM(cl.has_entry::INTEGER)                  AS total_entries,

            -- Subscriptions
            SUM(cl.has_subscription::INTEGER)           AS total_subscriptions,

            -- Billing
            SUM(cl.has_first_bill::INTEGER)             AS total_first_bills,

            -- Renewals (billing events that are NOT first bill)
            COALESCE((
                SELECT COUNT(*)
                FROM fct_billing fb
                JOIN dim_campaigns dc ON dc.campaign_id = fb.campaign_id
                WHERE fb.report_date = '{run_date}'
                  AND fb.is_first_bill = FALSE
                  AND fb.campaign_id = cl.campaign_id
            ), 0)                                       AS total_renewals,

            -- Revenue
            COALESCE((
                SELECT SUM(fb.amount)
                FROM fct_billing fb
                WHERE fb.report_date = '{run_date}'
                  AND fb.campaign_id = cl.campaign_id
                  AND fb.billing_status = 'SUCCESS'
            ), 0)                                       AS total_revenue,
            'GBP'                                       AS currency,

            -- Conversion rates (pre-computed, NULLIF avoids division by zero)
            ROUND(
                SUM(cl.has_subscription::INTEGER)::DECIMAL / NULLIF(COUNT(*), 0), 6
            )                                           AS sub_conversion_rate,
            ROUND(
                SUM(cl.has_first_bill::INTEGER)::DECIMAL / NULLIF(COUNT(*), 0), 6
            )                                           AS bill_conversion_rate,

            now()                                       AS loaded_at

        FROM fct_clicks cl
        WHERE cl.report_date = '{run_date}'
        GROUP BY 1, 2, 3, 4, 5
    """)

    count = conn.execute(f"SELECT COUNT(*) FROM mart_daily_performance WHERE report_date = '{run_date}'").fetchone()[0]
    logger.info(f"[mart_daily_performance] {count:,} rows inserted for {run_date}.")
    return count
