CREATE MATERIALIZED VIEW reporting.vw_top_products_per_brand AS
SELECT * 
FROM reporting.fn_top_products_per_brand(
    '2025-08-01',
    '2025-11-01',
    NULL        
);