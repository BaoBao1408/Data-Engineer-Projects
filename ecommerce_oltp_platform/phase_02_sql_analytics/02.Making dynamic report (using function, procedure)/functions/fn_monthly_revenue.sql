CREATE OR REPLACE FUNCTION reporting.fn_monthly_revenue(
    p_start_date DATE,
    p_end_date   DATE
)
RETURNS TABLE (
    month           DATE,
    total_orders    BIGINT,
    total_quantity  BIGINT,
    total_revenue   NUMERIC(18,2)
)
LANGUAGE sql
AS $$
SELECT
    date_trunc('month', o.order_date)::date AS month,
    COUNT(DISTINCT o.order_id)              AS total_orders,
    SUM(oi.quantity)                        AS total_quantity,
    SUM(oi.subtotal)                        AS total_revenue
FROM public.orders_partitioned o
JOIN public.order_item_partitioned oi
    ON o.order_id = oi.order_id
WHERE o.order_date >= p_start_date
  AND o.order_date <  p_end_date
GROUP BY 1
ORDER BY 1;
$$;
