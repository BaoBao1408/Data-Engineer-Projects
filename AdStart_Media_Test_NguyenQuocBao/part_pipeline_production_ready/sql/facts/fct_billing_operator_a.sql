-- sql/facts/fct_billing_operator_a.sql
-- Operator A billings: event_code = 2, status = SUCCESS.
-- billing_sequence / is_first_bill computed via window function.

DELETE FROM fct_billing WHERE report_date = :run_date AND operator = 'operator_A';

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
    a.event_time                    AS billed_at,
    a.event_time::DATE              AS report_date,
    (ROW_NUMBER() OVER (
        PARTITION BY a.msisdn, cl.campaign_id
        ORDER BY a.event_time
    ) = 1)                          AS is_first_bill,
    ROW_NUMBER() OVER (
        PARTITION BY a.msisdn, cl.campaign_id
        ORDER BY a.event_time
    )                               AS billing_sequence,
    'SUCCESS'                       AS billing_status,
    now()                           AS loaded_at
FROM raw_operator_a a
JOIN raw_clicks cl      ON cl.rotate_id  = a.rotate_id
JOIN dim_campaigns c    ON c.campaign_id = cl.campaign_id
LEFT JOIN fct_subscriptions sub
    ON  sub.msisdn    = a.msisdn
    AND sub.operator  = 'operator_A'
    AND sub.subscribed_at = (
        SELECT MAX(s2.subscribed_at)
        FROM fct_subscriptions s2
        WHERE s2.msisdn    = a.msisdn
          AND s2.operator  = 'operator_A'
          AND s2.subscribed_at <= a.event_time
    )
WHERE a.event_code = 2
  AND UPPER(a.status) = 'SUCCESS'
  AND a._loaded_date = :run_date;
