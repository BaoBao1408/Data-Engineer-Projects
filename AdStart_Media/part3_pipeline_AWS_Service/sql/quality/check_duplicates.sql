-- sql/quality/check_duplicates.sql
-- Detect duplicate primary keys in fact tables for run_date.
-- Each query MUST return 0 rows to pass.

-- check: fct_subscriptions source_transaction_id uniqueness
SELECT 'fct_sub_duplicate_txn' AS check_name, source_transaction_id, COUNT(*) AS cnt
FROM fct_subscriptions
WHERE report_date = :run_date
GROUP BY source_transaction_id
HAVING COUNT(*) > 1

UNION ALL

-- check: fct_billing source_transaction_id uniqueness
SELECT 'fct_bill_duplicate_txn', source_transaction_id, COUNT(*)
FROM fct_billing
WHERE report_date = :run_date
GROUP BY source_transaction_id
HAVING COUNT(*) > 1;

-- sql/quality/check_attribution_rate.sql
-- Layer 3: Alert when operator_C attribution rate drops below threshold.
--
-- Baseline: ~87% attributed (13% unattributed from SMS parser suffix bug).
-- Threshold: fail if unattributed rate > 20% — signals a regression or new parser bug.
--
-- Returns 0 rows → PASS.  Returns 1 row → FAIL (pipeline raises ValueError).
-- AWS: SNS alert fires via quality.py → ops team PagerDuty / Slack notification.

SELECT
    'operator_c_attribution_rate_below_threshold'   AS check_name,
    COUNT(*)                                        AS failing_rows
FROM (
    SELECT
        (
            SELECT COUNT(*)
            FROM raw_operator_c
            WHERE delivery_status = 'DELIVERED'
              AND _loaded_date    = :run_date
        )                                           AS total_delivered,
        (
            SELECT COUNT(*)
            FROM fct_unattributed_events
            WHERE operator    = 'operator_C'
              AND report_date = :run_date
        )                                           AS total_unattributed
) stats
-- Only fire when there IS data for this date (skip on empty / backfill days)
WHERE stats.total_delivered > 0
  AND CAST(stats.total_unattributed AS DOUBLE)
      / stats.total_delivered > 0.20
HAVING COUNT(*) > 0;
