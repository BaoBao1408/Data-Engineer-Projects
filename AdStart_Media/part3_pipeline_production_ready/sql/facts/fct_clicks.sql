-- sql/facts/fct_clicks.sql
-- Enrich clicks with boolean conversion-funnel flags.
-- All aggregations pre-computed for fast BI dashboard queries.

DELETE FROM fct_clicks WHERE report_date = :run_date;

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

    COALESCE(pv.has_page_view,  FALSE) AS has_page_view,
    COALESCE(pv.has_cta_click,  FALSE) AS has_cta_click,
    COALESCE(pv.has_entry,      FALSE) AS has_entry,
    COALESCE(sub.has_sub,       FALSE) AS has_subscription,
    COALESCE(bill.has_bill,     FALSE) AS has_first_bill,

    now()                           AS loaded_at
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

WHERE cl.clicked_at::DATE = :run_date;
