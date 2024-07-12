-- Query the database to find out which months have produced the most sales. The query should return the following information:

-- +-------------+-------+
-- | total_sales | month |
-- +-------------+-------+
-- |   673295.68 |     8 |
-- |   668041.45 |     1 |
-- |   657335.84 |    10 |
-- |   650321.43 |     5 |
-- |   645741.70 |     7 |
-- |   645463.00 |     3 |
-- +-------------+-------+
--SELECT * FROM dim_products
--SELECT * FROM dim_products WHERE product_code = 'R7-3126933h'
--SELECT product_price, date_added FROM dim_products
--SELECT * FROM orders_table
--SELECT * FROM dim_date_times
--SELECT * FROM orders_table WHERE date_uuid = '5929e936-5d4b-43db-b692-ba4f8e448e95'


SELECT
	ROUND(SUM(dp.product_price * ot.product_quantity)::numeric, 2) AS total_sales,
	ddt.month
FROM
	dim_products AS dp
JOIN
	orders_table AS ot
		ON ot.product_code = dp.product_code
JOIN
	dim_date_times AS ddt
		ON ddt.date_uuid = ot.date_uuid
GROUP BY
	month
ORDER BY
	total_sales DESC
	
