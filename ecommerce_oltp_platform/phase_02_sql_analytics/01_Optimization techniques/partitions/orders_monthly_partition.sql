{# 1. Create orders_partitioned #}

CREATE TABLE orders_partitioned (
    order_id     BIGINT,
    order_date   TIMESTAMP NOT NULL,
    seller_id    INT,
    status       VARCHAR(20),
    total_amount NUMERIC(12,2),
    created_at   TIMESTAMP
) PARTITION BY RANGE (order_date);

{# 2. Create monthly partitions (Aug–Oct 2025) #}
CREATE TABLE orders_2025_08 PARTITION OF orders_partitioned
FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

CREATE TABLE orders_2025_09 PARTITION OF orders_partitioned
FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE orders_2025_10 PARTITION OF orders_partitioned
FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

{# 3. Insert data #}
INSERT INTO orders_partitioned
SELECT *
FROM orders;

{# 4. Create order_item_partitioned #}
CREATE TABLE order_item_partitioned (
    order_item_id BIGINT,
    order_id      BIGINT,
    product_id    INT,
    order_date    TIMESTAMP NOT NULL,
    quantity      INT,
    unit_price    NUMERIC(10,2),
    subtotal      NUMERIC(12,2),
    created_at    TIMESTAMP
) PARTITION BY RANGE (order_date);

{# 5. Create monthly partitions #}
CREATE TABLE order_item_2025_08 PARTITION OF order_item_partitioned
FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

CREATE TABLE order_item_2025_09 PARTITION OF order_item_partitioned
FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE order_item_2025_10 PARTITION OF order_item_partitioned
FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

{# 6. Insert data #}
INSERT INTO order_item_partitioned
SELECT *
FROM order_item;    

{# 7. CREATE INDEX ON product_id #}
CREATE INDEX idx_order_item_product_id
ON order_item_partitioned (product_id);

{# 8. CHECK PARTITION PRUNING #}
EXPLAIN ANALYZE
SELECT *
FROM order_item_partitioned
WHERE order_date >= '2025-10-01'
  AND order_date <  '2025-11-01'
  AND product_id = 100;