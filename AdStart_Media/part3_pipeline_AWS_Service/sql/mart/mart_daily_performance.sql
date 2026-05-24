-- sql/mart/mart_daily_performance.sql
-- Pre-aggregated daily performance rollup consumed by BI tools (Metabase, Looker).
-- AWS: dbt model on Athena, or Redshift materialized view refresh.

DELETE FROM mart_daily_performance WHERE report_date = :run_date;

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

    -- Subscriptions + billing
    SUM(cl.has_subscription::INTEGER)           AS total_subscriptions,
    SUM(cl.has_first_bill::INTEGER)             AS total_first_bills,

    -- Renewals (billing events that are NOT the first bill)
    COALESCE((
        SELECT COUNT(*)
        FROM fct_billing fb
        WHERE fb.report_date  = :run_date
          AND fb.is_first_bill = FALSE
          AND fb.campaign_id  = cl.campaign_id
    ), 0)                                       AS total_renewals,

    -- Revenue (successful billings only)
    COALESCE((
        SELECT SUM(fb.amount)
        FROM fct_billing fb
        WHERE fb.report_date    = :run_date
          AND fb.campaign_id    = cl.campaign_id
          AND fb.billing_status = 'SUCCESS'
    ), 0)                                       AS total_revenue,
    'GBP'                                       AS currency,

    -- Pre-computed conversion rates (NULLIF avoids division-by-zero)
    ROUND(
        SUM(cl.has_subscription::INTEGER)::DECIMAL / NULLIF(COUNT(*), 0), 6
    )                                           AS sub_conversion_rate,
    ROUND(
        SUM(cl.has_first_bill::INTEGER)::DECIMAL  / NULLIF(COUNT(*), 0), 6
    )                                           AS bill_conversion_rate,

    now()                                       AS loaded_at

FROM fct_clicks cl
WHERE cl.report_date = :run_date
GROUP BY 1, 2, 3, 4, 5;
