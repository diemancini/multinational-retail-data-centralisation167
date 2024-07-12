-- The operations team would like to know the overall staff numbers in each location around the world. 
-- Perform a query to determine the staff numbers in each of the countries the company sells in.
-- The query should return the values:

-- +---------------------+--------------+
-- | total_staff_numbers | country_code |
-- +---------------------+--------------+
-- |               13307 | GB           |
-- |                6123 | DE           |
-- |                1384 | US           |
-- +---------------------+--------------+
--SELECT index, country_code, staff_numbers FROM dim_store_details WHERE staff_numbers is NULL

SELECT 
	SUM(staff_numbers) AS total_staff_numbers,
	country_code AS country
FROM 
	dim_store_details
GROUP BY 
	country_code
ORDER BY
	total_staff_numbers DESC;

