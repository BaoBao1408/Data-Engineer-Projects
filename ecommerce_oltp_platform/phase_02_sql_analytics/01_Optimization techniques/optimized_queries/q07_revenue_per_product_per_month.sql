EXPLAIN ANALYZE
    SELECT
        EXTRACT(MONTH FROM o.order_date) AS month,
        p.product_id,
        p.product_name,
        SUM(oi.subtotal) AS total_revenue
    FROM orders_partitioned o
    JOIN order_item_partitioned oi
        ON o.order_id = oi.order_id
    JOIN product p
        ON oi.product_id = p.product_id
    GROUP BY
        EXTRACT(MONTH FROM o.order_date),
        p.product_id,
        p.product_name
    ORDER BY
        month,
        total_revenue DESC;