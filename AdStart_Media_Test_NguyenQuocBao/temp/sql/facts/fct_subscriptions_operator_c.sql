-- sql/facts/fct_subscriptions_operator_c.sql
-- Operator C: DELIVERED = subscribe + charge in one event.
-- Attribution chain: tracking_code → raw_tracking_codes → rotate_id → clicks → campaign.
-- Rows where tracking_code cannot resolve are excluded here; they are logged in Python layer.

DELETE FROM fct_subscriptions WHERE report_date = :run_date AND operator = 'operator_C';

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
    'tracking_code_lookup'          AS attribution_method,
    now()                           AS loaded_at
FROM raw_operator_c oc
-- Lookup: match tracking_code within its 30-min validity window
JOIN raw_tracking_codes tc
    ON  tc.code          = oc.tracking_code
    AND oc.received_time BETWEEN tc.created_at AND tc.expired_at
JOIN raw_clicks cl      ON cl.rotate_id  = tc.rotate_id
JOIN dim_campaigns c    ON c.campaign_id = cl.campaign_id
WHERE oc.delivery_status = 'DELIVERED'
  AND oc._loaded_date = :run_date
  AND LENGTH(oc.tracking_code) <= 3;  -- guard against SMS parser suffix bug
