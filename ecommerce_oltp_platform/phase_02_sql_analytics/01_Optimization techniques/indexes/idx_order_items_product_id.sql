{# 1. CREATE INDEX ON product_id #}
CREATE INDEX idx_order_item_product_id
ON order_item_partitioned (product_id);

{# 2. CHECK PARTITION PRUNING #}
EXPLAIN ANALYZE
SELECT *
FROM order_item_partitioned
WHERE order_date >= '2025-10-01'
  AND order_date <  '2025-11-01'
  AND product_id = 100;