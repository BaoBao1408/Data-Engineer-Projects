-- sql/quality/check_mart.sql
-- Quality assertions run after mart build.
-- Each query MUST return 0 rows to pass.
-- AWS: run as a Lambda after Glue jobs complete alert via SNS on failure.

-- check: mart must have rows for run_date
SELECT 'mart_has_no_rows' AS check_name, COUNT(*) AS failing_rows
FROM mart_daily_performance
WHERE report_date = :run_date
HAVING COUNT(*) = 0

UNION ALL

-- check: no negative revenue
SELECT 'negative_revenue', COUNT(*)
FROM mart_daily_performance
WHERE report_date    = :run_date
  AND total_revenue  < 0

UNION ALL

-- check: conversion rates must be between 0 and 1
SELECT 'conversion_rate_over_100pct', COUNT(*)
FROM mart_daily_performance
WHERE report_date = :run_date
  AND (sub_conversion_rate > 1 OR bill_conversion_rate > 1)

UNION ALL

-- check: subscriptions cannot exceed clicks (basic sanity)
SELECT 'subscriptions_exceed_clicks', COUNT(*)
FROM mart_daily_performance
WHERE report_date          = :run_date
  AND total_subscriptions  > total_clicks;
