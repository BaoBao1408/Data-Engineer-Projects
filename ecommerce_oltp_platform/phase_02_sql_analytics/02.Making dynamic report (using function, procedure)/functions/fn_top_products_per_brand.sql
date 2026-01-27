CREATE OR REPLACE FUNCTION reporting.fn_top_products_per_brand(
    p_start_date DATE,
    p_end_date   DATE,
    p_seller_ids INT[] DEFAULT NULL
)
RETURNS TABLE (
    brand_id        INT,
    brand_name      TEXT,
    product_id      INT,
    product_name    TEXT,
    total_quantity  BIGINT,
    total_revenue   NUMERIC(18,2)
)
LANGUAGE sql
AS $$
SELECT
    b.brand_id,
    b.brand_name,
    p.product_id,
    p.product_name,
    SUM(oi.quantity) AS total_quantity,
    SUM(oi.subtotal) AS total_revenue
FROM orders_partitioned o
JOIN order_item_partitioned oi
    ON o.order_id = oi.order_id
JOIN product p
    ON oi.product_id = p.product_id
JOIN brand b
    ON p.brand_id = b.brand_id
WHERE o.order_date >= p_start_date
  AND o.order_date <  p_end_date
  AND (
        p_seller_ids IS NULL
        OR o.seller_id = ANY(p_seller_ids)
      )
GROUP BY
    b.brand_id, b.brand_name,
    p.product_id, p.product_name
ORDER BY
    b.brand_name,
    total_quantity DESC;
$$;
