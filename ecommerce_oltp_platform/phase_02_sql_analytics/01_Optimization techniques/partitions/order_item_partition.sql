{# 1. Create order_item_partitioned #}
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

{# 2. Create monthly partitions #}
CREATE TABLE order_item_2025_08 PARTITION OF order_item_partitioned
FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

CREATE TABLE order_item_2025_09 PARTITION OF order_item_partitioned
FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE order_item_2025_10 PARTITION OF order_item_partitioned
FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

{# 3. Insert data #}
INSERT INTO order_item_partitioned
SELECT *
FROM order_item;    

{# 4. Verify data insertion #}
SELECT COUNT(*) AS order_item_partitioned

SELECT * FROM order_item_2025_08