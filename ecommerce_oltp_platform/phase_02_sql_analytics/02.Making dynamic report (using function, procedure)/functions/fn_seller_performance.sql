CREATE OR REPLACE FUNCTION reporting.fn_seller_performance(
    p_start_date  DATE,
    p_end_date    DATE,
    p_brand_id    INT DEFAULT NULL,
    p_category_id INT DEFAULT NULL
)
RETURNS TABLE (
    seller_id       INT,
    seller_name     TEXT,
    total_orders    BIGINT,
    total_quantity  BIGINT,
    total_revenue   NUMERIC(18,2)
)
LANGUAGE sql
AS $$
SELECT
    s.seller_id,
    s.seller_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity)           AS total_quantity,
    SUM(oi.subtotal)           AS total_revenue
FROM public.orders_partitioned o
JOIN public.order_item_partitioned oi
    ON o.order_id = oi.order_id
JOIN public.seller s
    ON o.seller_id = s.seller_id
JOIN public.product p
    ON oi.product_id = p.product_id
WHERE o.order_date >= p_start_date
  AND o.order_date <  p_end_date
  AND (p_brand_id    IS NULL OR p.brand_id    = p_brand_id)
  AND (p_category_id IS NULL OR p.category_id = p_category_id)
GROUP BY s.seller_id, s.seller_name
ORDER BY total_revenue DESC;
$$;
