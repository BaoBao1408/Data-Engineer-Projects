CREATE MATERIALIZED VIEW reporting.vw_monthly_revenue AS
SELECT * FROM reporting.fn_monthly_revenue('2025-08-01', '2025-11-01')