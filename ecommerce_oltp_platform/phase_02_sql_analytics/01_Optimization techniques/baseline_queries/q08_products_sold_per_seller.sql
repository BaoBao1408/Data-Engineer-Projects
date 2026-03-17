EXPLAIN ANALYZE
	SELECT
	    s.seller_id,
	    s.seller_name,
	    SUM(oi.quantity) AS total_quantity_sold
	FROM orders o
	JOIN order_item oi
	    ON o.order_id = oi.order_id
	JOIN seller s
	    ON o.seller_id = s.seller_id
	GROUP BY
	    s.seller_id,
	    s.seller_name
	ORDER BY
	    total_quantity_sold DESC;