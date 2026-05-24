-- ═══════════════════════════════════════════════════════════════
-- sql/athena/facts/fct_subscriptions.sql
-- Dùng trong Athena Query Editor để inspect + debug fact table
-- ═══════════════════════════════════════════════════════════════

-- 1. Subscription count by operator for a given date
SELECT
    operator,
    COUNT(*)                                       AS total_subs,
    COUNT(DISTINCT msisdn)                         AS unique_msisdns,
    COUNT(DISTINCT campaign_id)                    AS campaigns,
    COUNT_IF(attribution_method = 'direct_rotate_id')    AS direct_attributed,
    COUNT_IF(attribution_method = 'tracking_code_lookup') AS tc_attributed
FROM adstart_warehouse.fct_subscriptions
WHERE report_date = '2026-01-15'
GROUP BY operator
ORDER BY total_subs DESC;


-- 2. Attribution breakdown per day (last 7 days)
SELECT
    report_date,
    operator,
    COUNT(*) AS subs
FROM adstart_warehouse.fct_subscriptions
WHERE report_date BETWEEN
    DATE_FORMAT(DATE_ADD('day', -7, CURRENT_DATE), '%Y-%m-%d')
    AND DATE_FORMAT(DATE_ADD('day', -1, CURRENT_DATE), '%Y-%m-%d')
GROUP BY report_date, operator
ORDER BY report_date, operator;


-- 3. Top campaigns by subscriptions
SELECT
    campaign_id,
    service_name,
    partner_id,
    COUNT(*)           AS subs,
    COUNT(DISTINCT msisdn) AS unique_users
FROM adstart_warehouse.fct_subscriptions
WHERE report_date = '2026-01-15'
GROUP BY campaign_id, service_name, partner_id
ORDER BY subs DESC
LIMIT 10;


-- 4. Subscription + billing join (conversion funnel)
SELECT
    s.campaign_id,
    s.operator,
    COUNT(DISTINCT s.subscription_id)  AS total_subs,
    COUNT(DISTINCT b.billing_id)       AS total_bills,
    COUNT(DISTINCT CASE WHEN b.is_first_bill THEN b.billing_id END) AS first_bills,
    ROUND(
        CAST(COUNT(DISTINCT b.billing_id) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT s.subscription_id), 0) * 100, 2
    ) AS bill_rate_pct
FROM adstart_warehouse.fct_subscriptions s
LEFT JOIN adstart_warehouse.fct_billing b
    ON  b.campaign_id  = s.campaign_id
    AND b.report_date  = s.report_date
WHERE s.report_date = '2026-01-15'
GROUP BY s.campaign_id, s.operator
ORDER BY total_subs DESC;
