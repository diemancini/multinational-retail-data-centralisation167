-- The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.
-- Find out the total and percentage of sales coming from each of the different store types.
-- The query should return:

-- +-------------+-------------+---------------------+
-- | store_type  | total_sales | percentage_total(%) |
-- +-------------+-------------+---------------------+
-- | Local       |  3440896.52 |               44.87 |
-- | Web portal  |  1726547.05 |               22.44 |
-- | Super Store |  1224293.65 |               15.63 |
-- | Mall Kiosk  |   698791.61 |                8.96 |
-- | Outlet      |   631804.81 |                8.10 |
-- +-------------+-------------+---------------------+

SELECT
	store_type,
	total_sales::numeric,
	ROUND(((total_sales / total_sales_all_stores)*100)::numeric,2) AS "percentage_total(%)"
FROM (
	SELECT
		dsd.store_type AS store_type,
		(
			SELECT
				SUM(dp.product_price * ot.product_quantity)::float
			FROM
				orders_table As ot
			JOIN
				dim_products AS dp
				ON dp.product_code = ot.product_code		
		) AS total_sales_all_stores,
		ROUND(SUM(dp.product_price * ot.product_quantity)::decimal, 2) As total_sales
	FROM
		orders_table As ot
	JOIN
		dim_store_details AS dsd
		ON dsd.store_code = ot.store_code
	JOIN
		dim_products AS dp
		ON dp.product_code = ot.product_code
	GROUP BY
		store_type
) AS sales_store
GROUP BY
	store_type, total_sales, total_sales_all_stores
ORDER BY
	total_sales DESC
	