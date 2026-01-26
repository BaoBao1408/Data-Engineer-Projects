EXPLAIN ANALYZE
    SELECT
        order_id
        order_date,
        seller_id,
        status,
        total_amount,
        created_at
    FROM orders_partitioned
    ORDER by total_amount DESC 
    LIMIT 1;