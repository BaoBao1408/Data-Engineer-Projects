-- ═══════════════════════════════════════════════════════════════
-- sql/athena/mart/mart_daily_performance.sql
-- Production BI queries — dùng trong Metabase / Looker / QuickSight
-- ═══════════════════════════════════════════════════════════════

-- 1. Daily KPI dashboard (primary query)
SELECT
    report_date,
    operator,
    campaign_id,
    service_name,
    partner_id,
    total_clicks,
    total_subscriptions,
    total_first_bills,
    total_renewals,
    ROUND(total_revenue, 2)                              AS revenue_gbp,
    ROUND(sub_conversion_rate  * 100, 2)                AS sub_cvr_pct,
    ROUND(bill_conversion_rate * 100, 2)                AS bill_cvr_pct,
    ROUND(total_revenue / NULLIF(total_clicks, 0), 4)   AS revenue_per_click
FROM adstart_warehouse.mart_daily_performance
WHERE report_date = '2026-01-15'
ORDER BY revenue_gbp DESC;


-- 2. Weekly revenue trend (last 4 weeks)
SELECT
    DATE_TRUNC('week', CAST(report_date AS DATE))   AS week_start,
    operator,
    SUM(total_clicks)         AS weekly_clicks,
    SUM(total_subscriptions)  AS weekly_subs,
    SUM(total_first_bills)    AS weekly_first_bills,
    ROUND(SUM(total_revenue), 2)  AS weekly_revenue,
    ROUND(AVG(sub_conversion_rate) * 100, 2) AS avg_sub_cvr_pct
FROM adstart_warehouse.mart_daily_performance
WHERE report_date >= DATE_FORMAT(DATE_ADD('day', -28, CURRENT_DATE), '%Y-%m-%d')
GROUP BY 1, 2
ORDER BY 1 DESC, weekly_revenue DESC;


-- 3. Campaign performance ranking (current month)
SELECT
    campaign_id,
    service_name,
    partner_id,
    SUM(total_clicks)          AS month_clicks,
    SUM(total_subscriptions)   AS month_subs,
    ROUND(SUM(total_revenue), 2) AS month_revenue,
    ROUND(AVG(sub_conversion_rate) * 100, 2) AS avg_cvr_pct,
    RANK() OVER (ORDER BY SUM(total_revenue) DESC) AS revenue_rank
FROM adstart_warehouse.mart_daily_performance
WHERE report_date >= DATE_FORMAT(DATE_TRUNC('month', CURRENT_DATE), '%Y-%m-%d')
GROUP BY campaign_id, service_name, partner_id
ORDER BY revenue_rank;


-- 4. Day-over-day comparison
WITH today AS (
    SELECT
        campaign_id,
        total_clicks        AS clicks_today,
        total_subscriptions AS subs_today,
        total_revenue       AS revenue_today
    FROM adstart_warehouse.mart_daily_performance
    WHERE report_date = '2026-01-15'
),
yesterday AS (
    SELECT
        campaign_id,
        total_clicks        AS clicks_yday,
        total_subscriptions AS subs_yday,
        total_revenue       AS revenue_yday
    FROM adstart_warehouse.mart_daily_performance
    WHERE report_date = '2026-01-14'
)
SELECT
    t.campaign_id,
    t.clicks_today,
    y.clicks_yday,
    ROUND((t.clicks_today - y.clicks_yday) * 100.0
          / NULLIF(y.clicks_yday, 0), 1)        AS clicks_chg_pct,
    ROUND(t.revenue_today, 2)                    AS revenue_today,
    ROUND(y.revenue_yday, 2)                     AS revenue_yday,
    ROUND((t.revenue_today - y.revenue_yday) * 100.0
          / NULLIF(y.revenue_yday, 0), 1)        AS revenue_chg_pct
FROM today t
LEFT JOIN yesterday y USING (campaign_id)
ORDER BY revenue_today DESC;


-- 5. Quality check: validate mart totals match fact tables
-- Chạy sau mỗi pipeline run để verify aggregation đúng
SELECT
    'mart_vs_facts' AS check_name,
    m.total_subs_mart,
    f.total_subs_fact,
    ABS(m.total_subs_mart - f.total_subs_fact) AS discrepancy,
    CASE WHEN m.total_subs_mart = f.total_subs_fact THEN 'PASS' ELSE 'FAIL' END AS status
FROM (
    SELECT SUM(total_subscriptions) AS total_subs_mart
    FROM adstart_warehouse.mart_daily_performance
    WHERE report_date = '2026-01-15'
) m
CROSS JOIN (
    SELECT COUNT(*) AS total_subs_fact
    FROM adstart_warehouse.fct_subscriptions
    WHERE report_date = '2026-01-15'
) f;
