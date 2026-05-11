-- sql/facts/fct_billing_operator_b.sql
-- Operator B REN rows (transaction_type = 'REN', amount > 0).
-- Key insight: REN has no rotate_id → must chain via msisdn to most recent SUB.

DELETE FROM fct_billing WHERE report_date = :run_date AND operator = 'operator_B';

INSERT INTO fct_billing
SELECT
    gen_random_uuid()               AS billing_id,
    'operator_B'                    AS operator,
    b.transaction_id                AS source_transaction_id,
    sub.subscription_id,
    c.campaign_id,
    c.service_name,
    c.partner_id,
    b.msisdn,
    COALESCE(b.amount, 0)           AS amount,
    'GBP'                           AS currency,
    b.created_at                    AS billed_at,
    b.created_at::DATE              AS report_date,
    (ROW_NUMBER() OVER (
        PARTITION BY b.msisdn, sub.campaign_id
        ORDER BY b.created_at
    ) = 1)                          AS is_first_bill,
    ROW_NUMBER() OVER (
        PARTITION BY b.msisdn, sub.campaign_id
        ORDER BY b.created_at
    )                               AS billing_sequence,
    'SUCCESS'                       AS billing_status,
    now()                           AS loaded_at
FROM raw_operator_b b
-- REN has no rotate_id → resolve via msisdn to most recent prior SUB
LEFT JOIN fct_subscriptions sub
    ON  sub.msisdn    = b.msisdn
    AND sub.operator  = 'operator_B'
    AND sub.subscribed_at = (
        SELECT MAX(s2.subscribed_at)
        FROM fct_subscriptions s2
        WHERE s2.msisdn    = b.msisdn
          AND s2.operator  = 'operator_B'
          AND s2.subscribed_at <= b.created_at
    )
LEFT JOIN dim_campaigns c ON c.campaign_id = sub.campaign_id
WHERE b.transaction_type = 'REN'
  AND COALESCE(b.amount, 0) > 0
  AND b._loaded_date = :run_date
  AND c.campaign_id IS NOT NULL;
