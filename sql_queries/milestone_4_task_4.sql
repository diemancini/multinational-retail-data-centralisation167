-- The company is looking to increase its online sales.
-- They want to know how many sales are happening online vs offline.
-- Calculate how many products were sold and the amount of sales made for online and offline purchases.
-- You should get the following information:

-- +------------------+-------------------------+----------+
-- | numbers_of_sales | product_quantity_count  | location |
-- +------------------+-------------------------+----------+
-- |            26957 |                  107739 | Web      |
-- |            93166 |                  374047 | Offline  |
-- +------------------+-------------------------+----------+

--SELECT * FROM orders_table
--SELECT * FROM orders_table WHERE store_code LIKE 'WEB%'
--SELECT * FROM dim_products
--SELECT * FROM dim_store_details
--SELECT * FROM dim_store_details WHERE store_type LIKE 'Web%'

-- SELECT a,
--    CASE WHEN a=1 THEN 'one'
-- 		WHEN a=2 THEN 'two'
-- 		ELSE 'other'
--    END
-- FROM test;
SELECT
	SUM(number_of_sales) as number_of_sales,
	SUM(product_quantity_count) as product_quantity_count,
	location
FROM (
	SELECT
		COUNT(ot.store_code) AS number_of_sales,
		SUM(ot.product_quantity) AS product_quantity_count,
		dsd.store_type,
			CASE
				dsd.store_type 
				WHEN 'Mall Kiosk' THEN 'Offline'
				WHEN 'Super Store' THEN 'Offline'
				WHEN 'Local' THEN 'Offline'
				WHEN 'Outlet' THEN 'Offline'
				ELSE 'Web'
			END
		AS location
	FROM 
		orders_table AS ot
	JOIN
		dim_store_details AS dsd
			ON dsd.store_code = ot.store_code
	GROUP BY
		dsd.store_type
) AS on_off_line
GROUP BY
	location
ORDER BY
	location DESC
	