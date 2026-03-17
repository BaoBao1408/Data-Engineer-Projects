EXPLAIN ANALYZE
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        subtotal,
        order_date
    FROM order_item_partitioned
    WHERE product_id > 50 and product_id <= 100;