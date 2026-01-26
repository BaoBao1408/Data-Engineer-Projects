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

{# 4. Verify data insertion #}
SELECT COUNT(*) AS total_orders_partitioned

SELECT * FROM orders_2025_08