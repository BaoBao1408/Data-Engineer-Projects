EXPLAIN ANALYSE
	SELECT date_trunc('month', order_date) as month,
		sum(total_amount) as revenue
	FROM orders
	GROUP by 1
	ORDER by 1