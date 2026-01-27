CREATE MATERIALIZED VIEW reporting.vw_order_status_summary AS
SELECT * 
FROM reporting.fn_order_status_summary(
    '2025-08-01',
    '2025-11-01',
    NULL        
);