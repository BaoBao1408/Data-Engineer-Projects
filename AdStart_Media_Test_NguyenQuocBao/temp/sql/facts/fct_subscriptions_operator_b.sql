-- sql/facts/fct_subscriptions_operator_b.sql
-- Operator B SUB rows only. rotate_id present → direct attribution.
-- REN rows (no rotate_id) are handled in fct_billing via msisdn chain.

DELETE FROM fct_subscriptions WHERE report_date = :run_date AND operator = 'operator_B';

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
JOIN raw_clicks cl      ON cl.rotate_id  = b.rotate_id
JOIN dim_campaigns c    ON c.campaign_id = cl.campaign_id
WHERE b.transaction_type = 'SUB'
  AND b._loaded_date = :run_date
  AND b.rotate_id IS NOT NULL;
