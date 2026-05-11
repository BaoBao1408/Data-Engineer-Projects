-- sql/facts/fct_subscriptions_operator_a.sql
-- Operator A: event_code = 1 → subscribe. rotate_id present → direct attribution.

DELETE FROM fct_subscriptions WHERE report_date = :run_date AND operator = 'operator_A';

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
    a.event_time                    AS subscribed_at,
    a.event_time::DATE              AS report_date,
    'direct_rotate_id'              AS attribution_method,
    now()                           AS loaded_at
FROM raw_operator_a a
JOIN raw_clicks cl      ON cl.rotate_id   = a.rotate_id
JOIN dim_campaigns c    ON c.campaign_id  = cl.campaign_id
WHERE a.event_code = 1
  AND a._loaded_date = :run_date
  AND a.rotate_id IS NOT NULL;
