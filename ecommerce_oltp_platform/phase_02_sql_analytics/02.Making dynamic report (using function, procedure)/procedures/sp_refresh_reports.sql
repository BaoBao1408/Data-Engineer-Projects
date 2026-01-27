CREATE OR REPLACE PROCEDURE reporting.sp_refresh_reports()
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.vw_monthly_revenue;
    REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.vw_seller_performance;
    REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.vw_order_status_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.vw_daily_revenue;
    REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.vw_top_products_per_brand;
    -- Placeholder for future materialized reports
    -- Example:
    -- REFRESH MATERIALIZED VIEW reporting.mv_monthly_revenue;

    RAISE NOTICE 'Reporting refresh completed at %', now();
END;
$$;
