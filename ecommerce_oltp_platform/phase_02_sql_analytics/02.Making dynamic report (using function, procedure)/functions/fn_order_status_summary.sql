CREATE OR REPLACE FUNCTION reporting.fn_order_status_summary(
    p_start_date  DATE,
    p_end_date    DATE,
    p_seller_ids  INT[] DEFAULT NULL,
    p_category_ids INT[] DEFAULT NULL
)
RETURNS TABLE (
    status          TEXT,
    total_orders    BIGINT,
    total_revenue   NUMERIC(18,2)
)
LANGUAGE sql
AS $$
SELECT
    o.status,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount)        AS total_revenue
FROM orders_partitioned o
JOIN order_item_partitioned oi
    ON o.order_id = oi.order_id
JOIN product p
    ON oi.product_id = p.product_id
WHERE o.order_date >= p_start_date
  AND o.order_date <  p_end_date
  AND (
        p_seller_ids IS NULL
        OR o.seller_id = ANY(p_seller_ids)
      )
  AND (
        p_category_ids IS NULL
        OR p.category_id = ANY(p_category_ids)
      )
GROUP BY o.status
ORDER BY total_orders DESC;
$$;
