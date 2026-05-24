-- ═══════════════════════════════════════════════════════════════
-- sql/athena/quality/data_quality_checks.sql
-- Ad-hoc quality checks — chạy thủ công trong Athena console
-- để debug khi pipeline báo fail
-- ═══════════════════════════════════════════════════════════════

-- ── 1. Duplicate primary keys ────────────────────────────────────

-- Tìm duplicate subscription IDs
SELECT source_transaction_id, COUNT(*) AS cnt
FROM adstart_warehouse.fct_subscriptions
WHERE report_date = '2026-01-15'
GROUP BY source_transaction_id
HAVING COUNT(*) > 1;

-- Tìm duplicate billing transaction IDs
SELECT source_transaction_id, operator, COUNT(*) AS cnt
FROM adstart_warehouse.fct_billing
WHERE report_date = '2026-01-15'
GROUP BY source_transaction_id, operator
HAVING COUNT(*) > 1;


-- ── 2. Null rate check ───────────────────────────────────────────

-- Kiểm tra null rate cho các critical columns
SELECT
    COUNT(*)                                                          AS total_rows,
    ROUND(COUNT_IF(msisdn IS NULL)      * 100.0 / COUNT(*), 2)      AS msisdn_null_pct,
    ROUND(COUNT_IF(campaign_id IS NULL) * 100.0 / COUNT(*), 2)      AS campaign_null_pct,
    ROUND(COUNT_IF(rotate_id IS NULL)   * 100.0 / COUNT(*), 2)      AS rotate_id_null_pct,
    ROUND(COUNT_IF(subscribed_at IS NULL) * 100.0 / COUNT(*), 2)    AS subscribed_at_null_pct
FROM adstart_warehouse.fct_subscriptions
WHERE report_date = '2026-01-15';


-- ── 3. operator_C attribution analysis ───────────────────────────

-- Tổng DELIVERED events vs attributed vs unattributed
SELECT
    'delivered'    AS category,
    COUNT(*)       AS event_count
FROM adstart_raw.raw_operator_c
WHERE delivery_status = 'DELIVERED'
  AND _loaded_date = '2026-01-15'

UNION ALL

SELECT
    'attributed'   AS category,
    COUNT(*)       AS event_count
FROM adstart_warehouse.fct_subscriptions
WHERE operator = 'operator_C'
  AND report_date = '2026-01-15'

UNION ALL

SELECT
    'unattributed' AS category,
    COUNT(*)       AS event_count
FROM adstart_warehouse.fct_unattributed_events
WHERE operator = 'operator_C'
  AND report_date = '2026-01-15';


-- Breakdown unattributed reasons
SELECT
    unattributed_reason,
    COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM adstart_warehouse.fct_unattributed_events
WHERE report_date = '2026-01-15'
GROUP BY unattributed_reason
ORDER BY cnt DESC;


-- ── 4. Revenue sanity checks ─────────────────────────────────────

-- Negative revenue check
SELECT COUNT(*) AS negative_revenue_rows
FROM adstart_warehouse.mart_daily_performance
WHERE report_date = '2026-01-15'
  AND total_revenue < 0;

-- Conversion rate > 100% check (logical impossibility)
SELECT campaign_id, sub_conversion_rate, bill_conversion_rate
FROM adstart_warehouse.mart_daily_performance
WHERE report_date = '2026-01-15'
  AND (sub_conversion_rate > 1 OR bill_conversion_rate > 1);

-- Subs > clicks check (funnel violation)
SELECT
    campaign_id,
    total_clicks,
    total_subscriptions,
    total_subscriptions - total_clicks AS excess
FROM adstart_warehouse.mart_daily_performance
WHERE report_date = '2026-01-15'
  AND total_subscriptions > total_clicks;


-- ── 5. Cross-date consistency ────────────────────────────────────

-- Kiểm tra không có data missing cho last 7 ngày
SELECT
    report_date,
    COUNT(*) AS campaign_rows
FROM adstart_warehouse.mart_daily_performance
WHERE report_date >= DATE_FORMAT(DATE_ADD('day', -7, CURRENT_DATE), '%Y-%m-%d')
GROUP BY report_date
ORDER BY report_date;

-- Revenue variance ngày hôm qua vs 7 ngày trước (>50% change = cần investigate)
WITH daily_revenue AS (
    SELECT
        report_date,
        SUM(total_revenue) AS total_rev
    FROM adstart_warehouse.mart_daily_performance
    WHERE report_date >= DATE_FORMAT(DATE_ADD('day', -8, CURRENT_DATE), '%Y-%m-%d')
    GROUP BY report_date
    ORDER BY report_date DESC
    LIMIT 8
)
SELECT
    report_date,
    total_rev,
    LAG(total_rev) OVER (ORDER BY report_date)      AS prev_day_rev,
    ROUND(
        (total_rev - LAG(total_rev) OVER (ORDER BY report_date))
        * 100.0 / NULLIF(LAG(total_rev) OVER (ORDER BY report_date), 0),
    2) AS day_over_day_change_pct
FROM daily_revenue
ORDER BY report_date DESC;
