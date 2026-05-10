-- sql/facts/fct_billing_operator_c.sql
-- Operator C: DELIVERED = subscription + first charge combined.
-- Amount is always 0 (operator does not report revenue).

DELETE FROM fct_billing WHERE report_date = :run_date AND operator = 'operator_C';

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
    0.00                            AS amount,   -- operator_C does not report revenue
    'GBP'                           AS currency,
    oc.received_time                AS billed_at,
    oc.received_time::DATE          AS report_date,
    TRUE                            AS is_first_bill,
    1                               AS billing_sequence,
    'SUCCESS'                       AS billing_status,
    now()                           AS loaded_at
FROM raw_operator_c oc
JOIN fct_subscriptions sub
    ON  sub.source_transaction_id = oc.message_id
    AND sub.operator = 'operator_C'
WHERE oc.delivery_status = 'DELIVERED'
  AND oc._loaded_date = :run_date;
