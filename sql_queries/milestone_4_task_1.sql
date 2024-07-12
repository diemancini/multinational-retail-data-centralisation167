-- The Operations team would like to know which countries we currently operate in and which country now has the most stores. Perform a query on the database to get the information, it should return the following information:

-- +----------+-----------------+
-- | country  | total_no_stores |
-- +----------+-----------------+
-- | GB       |             265 |
-- | DE       |             141 |
-- | US       |              34 |
-- +----------+-----------------+
-- Note: DE is short for Deutschland(Germany)

SELECT 
	country_code AS country,
	COUNT(country_code) AS total_no_stores
FROM 
	dim_store_details
GROUP BY 
	country_code
ORDER BY
	total_no_stores DESC