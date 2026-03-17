EXPLAIN ANALYSE
	SELECT date_trunc('month', order_date) as month,
		s.seller_name,
		sum(total_amount) as revenue
	FROM orders_partitioned o 
	JOIN seller s
		ON o.seller_id = s.seller_id
	GROUP by 1, 2
	ORDER by 2, 1