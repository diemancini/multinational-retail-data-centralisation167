-- The company stakeholders want assurances that the company has been doing well recently.

-- Find which months in which years have had the most sales historically.

-- The query should return the following information:

-- +-------------+------+-------+
-- | total_sales | year | month |
-- +-------------+------+-------+
-- |    27936.77 | 1994 |     3 |
-- |    27356.14 | 2019 |     1 |
-- |    27091.67 | 2009 |     8 |
-- |    26679.98 | 1997 |    11 |
-- |    26310.97 | 2018 |    12 |
-- |    26277.72 | 2019 |     8 |
-- |    26236.67 | 2017 |     9 |
-- |    25798.12 | 2010 |     5 |
-- |    25648.29 | 1996 |     8 |
-- |    25614.54 | 2000 |     1 |
-- +-------------+------+-------+

SELECT
  ROUND(SUM(dp.product_price * ot.product_quantity)::numeric, 2) AS total_sales,
  year,
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
  year, month
ORDER BY
  total_sales DESC
LIMIT 10
