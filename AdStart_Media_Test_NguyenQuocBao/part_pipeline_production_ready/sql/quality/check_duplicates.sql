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
