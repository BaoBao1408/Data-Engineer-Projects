EXPLAIN ANALYZE
	SELECT
	    seller_id,
	    COUNT(*) AS total_orders
	FROM orders
	WHERE order_date >= '2025-10-01'
	  AND order_date <  '2025-11-01'
	GROUP BY seller_id;
