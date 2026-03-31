CREATE MATERIALIZED VIEW reporting.vw_daily_revenue AS
SELECT * FROM reporting.fn_daily_revenue(
    '2025-10-01',
    '2025-10-31',
    ARRAY[100,101,102]
);