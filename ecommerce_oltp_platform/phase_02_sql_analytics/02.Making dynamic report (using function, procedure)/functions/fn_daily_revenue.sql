CREATE OR REPLACE FUNCTION reporting.fn_daily_revenue(
    p_start_date DATE,
    p_end_date   DATE,
    p_product_ids INT[] DEFAULT NULL
)
RETURNS TABLE (
    date            DATE,
    total_orders    BIGINT,
    total_quantity  BIGINT,
    total_revenue   NUMERIC(18,2)
)
LANGUAGE sql
AS $$
SELECT
    o.order_date::date              AS date,
    COUNT(DISTINCT o.order_id)      AS total_orders,
    SUM(oi.quantity)                AS total_quantity,
    SUM(oi.subtotal)                AS total_revenue
FROM public.orders_partitioned o
JOIN public.order_item_partitioned oi
    ON o.order_id = oi.order_id
WHERE o.order_date >= p_start_date
  AND o.order_date <  p_end_date
  AND (
        p_product_ids IS NULL
        OR oi.product_id = ANY(p_product_ids)
      )
GROUP BY 1
ORDER BY 1;
$$;
