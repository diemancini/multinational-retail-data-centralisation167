-- The sales team is looking to expand their territory in Germany. 
-- Determine which type of store is generating the most sales in Germany.
-- The query will return:

-- +--------------+-------------+--------------+
-- | total_sales  | store_type  | country_code |
-- +--------------+-------------+--------------+
-- |   198373.57  | Outlet      | DE           |
-- |   247634.20  | Mall Kiosk  | DE           |
-- |   384625.03  | Super Store | DE           |
-- |  1109909.59  | Local       | DE           |
-- +--------------+-------------+--------------+

SELECT
	ROUND(SUM(dp.product_price * ot.product_quantity)::decimal, 2) As total_sales,
	dsd.store_type AS store_type,
	dsd.country_code
FROM
	orders_table As ot
JOIN
	dim_store_details AS dsd
	ON dsd.store_code = ot.store_code
JOIN
	dim_products AS dp
	ON dp.product_code = ot.product_code
WHERE
	dsd.country_code = 'DE'
GROUP BY
	store_type, country_code
ORDER BY
	total_sales ASC
	