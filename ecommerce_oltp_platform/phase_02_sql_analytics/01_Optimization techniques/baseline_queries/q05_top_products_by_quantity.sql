EXPLAIN ANALYZE
	SELECT
	    product_id,
	    SUM(quantity) AS total_quantity
	FROM order_item
	GROUP BY product_id
	ORDER BY total_quantity DESC
	LIMIT 10;