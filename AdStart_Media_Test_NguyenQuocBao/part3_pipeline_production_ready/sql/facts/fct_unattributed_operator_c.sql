-- sql/facts/fct_unattributed_operator_c.sql
-- Layer 1: Quarantine — capture operator_C DELIVERED rows that could not be attributed.
-- Two reasons are tracked separately so ops teams can triage source vs config issues.
-- IDEMPOTENCY: DELETE for run_date + operator before re-inserting.
-- AWS: write to s3://adstart-raw/unattributed/operator=C/date=:run_date/ via awswrangler.

DELETE FROM fct_unattributed_events
WHERE report_date = :run_date
  AND operator    = 'operator_C';

-- ── Reason 1: SMS parser suffix bug — tracking_code longer than 3 chars ──────────────
INSERT INTO fct_unattributed_events
SELECT
    gen_random_uuid()           AS event_id,
    'operator_C'                AS operator,
    'raw_operator_c'            AS source_table,
    oc.msisdn,
    oc.tracking_code            AS raw_tracking_code,
    oc.received_time            AS event_time,
    oc.received_time::DATE      AS report_date,
    'tracking_code_too_long'    AS unattributed_reason,
    now()                       AS loaded_at
FROM raw_operator_c oc
WHERE oc.delivery_status = 'DELIVERED'
  AND oc._loaded_date    = :run_date
  AND LENGTH(oc.tracking_code) > 3;

-- ── Reason 2: Valid-length code but no match in raw_tracking_codes ────────────────────
-- Covers: code expired (event arrived after the 30-min window) or code never existed.
INSERT INTO fct_unattributed_events
SELECT
    gen_random_uuid()               AS event_id,
    'operator_C'                    AS operator,
    'raw_operator_c'                AS source_table,
    oc.msisdn,
    oc.tracking_code                AS raw_tracking_code,
    oc.received_time                AS event_time,
    oc.received_time::DATE          AS report_date,
    'no_matching_tracking_code'     AS unattributed_reason,
    now()                           AS loaded_at
FROM raw_operator_c oc
LEFT JOIN raw_tracking_codes tc
    ON  tc.code          = oc.tracking_code
    AND oc.received_time BETWEEN tc.created_at AND tc.expired_at
WHERE oc.delivery_status = 'DELIVERED'
  AND oc._loaded_date    = :run_date
  AND LENGTH(oc.tracking_code) <= 3   -- already-clean codes only
  AND tc.rotate_id IS NULL;            -- but no match found
