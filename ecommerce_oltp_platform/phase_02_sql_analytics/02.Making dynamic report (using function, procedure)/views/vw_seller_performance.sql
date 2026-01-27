CREATE MATERIALIZED VIEW reporting.vw_seller_performance AS
SELECT * FROM reporting.fn_seller_performance(
    '2025-08-01',
    '2025-11-01',
    NULL,
    NULL
);