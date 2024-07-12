-- Sales would like the get an accurate metric for how quickly the company is making sales.

-- Determine the average time taken between each sale grouped by year, the query should return the following information:

--  +------+-------------------------------------------------------+
--  | year |                           actual_time_taken           |
--  +------+-------------------------------------------------------+
--  | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
--  | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |
--  | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... | 
--  | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
--  | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
--  +------+-------------------------------------------------------+
--SELECT * FROM dim_date_times WHERE day='19' and month='9' and year='2012' ORDER BY timestamp
--SELECT * FROM dim_date_times WHERE year IS NOT NULL ORDER BY year, month, day, timestamp

WITH sort_sales_times AS (
	SELECT
		timestamp,
		day,	
		month,
		year, 
		LEAD(timestamp, 1) OVER (
			ORDER BY year, month, day, timestamp
		) AS next_hour_sale,
		LEAD(day, 1) OVER (
			ORDER BY year, month, day, timestamp
		) AS next_day_sales
	FROM
		dim_date_times
), excluding_end_sales_days AS (
	SELECT
		timestamp,
		day,	
		month,
		year,
		next_hour_sale,
		next_day_sales
	FROM
		sort_sales_times
	WHERE
		day = next_day_sales AND year is NOT NULL
), convert_to_timestamp_sales AS (
	SELECT
		year,
		TO_TIMESTAMP(year||'-'||month||'-'||day|| ' ' || timestamp, 'YYYY/MM/DD/HH24:MI:ss') AS convert_timestamp,
		TO_TIMESTAMP(year||'-'||month||'-'||day|| ' ' || next_hour_sale, 'YYYY/MM/DD/HH24:MI:ss') AS convert_next_hour_sale
	FROM
		excluding_end_sales_days
), calculating_time_between_sales AS (
	SELECT
		year,
		AVG((convert_next_hour_sale - convert_timestamp)) AS actual_time_taken
	FROM
		convert_to_timestamp_sales
	GROUP BY
		year
	ORDER BY
		actual_time_taken DESC
), extract_time_from_actual_time_taken AS (
	SELECT
		year,
		actual_time_taken,
		EXTRACT (HOUR FROM actual_time_taken) AS hours,
		EXTRACT (MINUTE FROM actual_time_taken) AS minutes,
		floor(EXTRACT (SECOND FROM actual_time_taken)) AS seconds,
		(EXTRACT (SECOND FROM actual_time_taken))::numeric % 1::numeric AS milliseconds
	FROM
		calculating_time_between_sales
), convert_time_to_string AS (
	SELECT
		year,
		('"hours": '||hours||', '||'"minutes": '||minutes||', '||'"seconds": '||seconds||', '||'"milliseconds": '||milliseconds) AS actual_time_taken
	FROM
		extract_time_from_actual_time_taken
	ORDER BY
		actual_time_taken DESC
)

SELECT * FROM convert_time_to_string
--SELECT * FROM extract_time_from_actual_time_taken
--SELECT * FROM convert_to_timestamp_sales
-- select 'Join these ' || 'strings with a number ' || 23;
--SELECT * FROM sort_sales_times
--SELECT * FROM excluding_end_sales_days
--SELECT CURRENT_DATE + INTERVAL '1 month';
-- SELECT TO_TIMESTAMP( '15:23:45', 'YYYY/MM/DD/HH24:MI:ss')
--          AS new_timestamptz;
