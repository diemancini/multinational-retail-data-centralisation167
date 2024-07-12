from database_manager.database_utils import DatabaseConnector

from pandas import DataFrame

START = 0
FINISH = -1


class Queries(DatabaseConnector):

    def get_total_number_stores_by_country(self, number_dash: int) -> None:
        """
        TASK 1 in MILESTONE 4

        Returns:
            - Should return a table as shows below:
            +----------+-----------------+
            | country  | total_no_stores |
            +----------+-----------------+
            | GB       |             265 |
            | DE       |             141 |
            | US       |              34 |
            +----------+-----------------+
        """
        print(f"-" * number_dash)
        select_query: str = """
            SELECT 
                country_code AS country,
                COUNT(country_code) AS total_no_stores
            FROM 
                dim_store_details
            GROUP BY 
                country_code
            ORDER BY
                total_no_stores DESC;
        """
        df: DataFrame = self.select_db(select_query)
        print(df)

    def get_total_number_stores_by_locality(self, number_dash: int) -> None:
        """
        TASK 2 in MILESTONE 4

        Returns:
            - Should return a table as shows below:
            +-------------------+-----------------+
            |     locality      | total_no_stores |
            +-------------------+-----------------+
            | Chapletown        |              14 |
            | Belper            |              13 |
            | Bushley           |              12 |
            | Exeter            |              11 |
            | High Wycombe      |              10 |
            | Arbroath          |              10 |
            | Rutherglen        |              10 |
            +-------------------+-----------------+
        """
        print(f"-" * number_dash)
        select_query: str = """
            SELECT 
                locality,
                COUNT(locality) AS total_no_stores
            FROM 
                dim_store_details
            GROUP BY 
                locality
            HAVING
                COUNT(locality) > 9
            ORDER BY
                total_no_stores DESC
        """
        df: DataFrame = self.select_db(select_query)
        print(df)

    def get_most_sales_per_month(self, number_dash: int) -> None:
        """
        TASK 3 in MILESTONE 4

        Returns:
            - Should return a table as shows below:
            +-------------+-------+
            | total_sales | month |
            +-------------+-------+
            |   673295.68 |     8 |
            |   668041.45 |     1 |
            |   657335.84 |    10 |
            |   650321.43 |     5 |
            |   645741.70 |     7 |
            |   645463.00 |     3 |
            +-------------+-------+
        """
        print(f"-" * number_dash)
        select_query: str = """
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
        """
        df: DataFrame = self.select_db(select_query)
        print(df)

    def get_count_sales_by_on_off_line(self, number_dash: int) -> None:
        """
        TASK 4 in MILESTONE 4

        Returns:
            - Should return a table as shows below:
            +------------------+-------------------------+----------+
            | numbers_of_sales | product_quantity_count  | location |
            +------------------+-------------------------+----------+
            |            26957 |                  107739 | Web      |
            |            93166 |                  374047 | Offline  |
            +------------------+-------------------------+----------+
        """
        print(f"-" * number_dash)
        select_query: str = """
          SELECT
              SUM(number_of_sales)::integer as number_of_sales,
              SUM(product_quantity_count)::integer as product_quantity_count,
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
        """
        df: DataFrame = self.select_db(select_query)
        print(df)

    def get_total_sales_by_store_type(self, number_dash: int) -> None:
        """
        TASK 5 in MILESTONE 4

        +-------------+-------------+---------------------+
        | store_type  | total_sales | percentage_total(%) |
        +-------------+-------------+---------------------+
        | Local       |  3440896.52 |               44.87 |
        | Web portal  |  1726547.05 |               22.44 |
        | Super Store |  1224293.65 |               15.63 |
        | Mall Kiosk  |   698791.61 |                8.96 |
        | Outlet      |   631804.81 |                8.10 |
        +-------------+-------------+---------------------+
        """
        print(f"-" * number_dash)
        select_query: str = """
          SELECT
              store_type,
              total_sales::numeric,
              ROUND(((total_sales / total_sales_all_stores)*100)::numeric, 2) AS percentage_total
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
                  ROUND(SUM(dp.product_price * ot.product_quantity)::numeric, 2) As total_sales
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
        """
        df: DataFrame = self.select_db(select_query)
        print(df)

    def get_most_sales_per_month_and_year(self, number_dash: int) -> None:
        """
        TASK 6 in MILESTONE 4

        +-------------+------+-------+
        | total_sales | year | month |
        +-------------+------+-------+
        |    27936.77 | 1994 |     3 |
        |    27356.14 | 2019 |     1 |
        |    27091.67 | 2009 |     8 |
        |    26679.98 | 1997 |    11 |
        |    26310.97 | 2018 |    12 |
        |    26277.72 | 2019 |     8 |
        |    26236.67 | 2017 |     9 |
        |    25798.12 | 2010 |     5 |
        |    25648.29 | 1996 |     8 |
        |    25614.54 | 2000 |     1 |
        +-------------+------+-------+
        """
        print(f"-" * number_dash)
        select_query: str = """
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
        """
        df: DataFrame = self.select_db(select_query)
        print(df)

    def get_total_staff_numbers_by_country(self, number_dash: int) -> None:
        """
        TASK 7 in MILESTONE 4
        +---------------------+--------------+
        | total_staff_numbers | country_code |
        +---------------------+--------------+
        |               13307 | GB           |
        |                6123 | DE           |
        |                1384 | US           |
        +---------------------+--------------+
        """
        print(f"-" * number_dash)
        select_query: str = """
          SELECT 
              SUM(staff_numbers) AS total_staff_numbers,
              country_code AS country
          FROM 
              dim_store_details
          GROUP BY 
              country_code
          ORDER BY
              total_staff_numbers DESC;
        """
        df: DataFrame = self.select_db(select_query)
        print(df)

    def get_total_sales_by_store_type_in_germany(self, number_dash: int) -> None:
        """
        TASK 8 in MILESTONE 4
        +--------------+-------------+--------------+
        | total_sales  | store_type  | country_code |
        +--------------+-------------+--------------+
        |   198373.57  | Outlet      | DE           |
        |   247634.20  | Mall Kiosk  | DE           |
        |   384625.03  | Super Store | DE           |
        |  1109909.59  | Local       | DE           |
        +--------------+-------------+--------------+
        """
        print(f"-" * number_dash)
        select_query: str = """
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
        """
        df: DataFrame = self.select_db(select_query)
        print(df)

    def get_average_time_taken_between_each_sale(self, number_dash: int) -> None:
        """
        TASK  in MILESTONE 4
        +------+-------------------------------------------------------+
        | year |                           actual_time_taken           |
        +------+-------------------------------------------------------+
        | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
        | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |
        | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... |
        | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
        | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
        +------+-------------------------------------------------------+
        """
        print(f"-" * number_dash)
        select_query: str = """
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
                (EXTRACT (SECOND FROM actual_time_taken)) - floor((EXTRACT (SECOND FROM actual_time_taken))) AS milliseconds
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
        """
        df: DataFrame = self.select_db(select_query)
        print(f"{df}")


if __name__ == "__main__":

    answer = START
    while answer != FINISH:
        try:
            text = (
                ""
                "Please, which query do you want to be executed?\n\n"
                "0 - CLOSE THE PROGRAMM\n"
                "1 - TOTAL NUMBER OF STORES BY COUNTRY\n"
                "2 - TOTAL NUMBER STORES BY LOCALITY\n"
                "3 - MOST SALES PER MONTH\n"
                "4 - COUNT SALES BY ON OFF LINE\n"
                "5 - TOTAL SALES BY STORE TYPE\n"
                "6 - MOST SALES PER MONTH AND YEAR\n"
                "7 - TOTAL STAFF NUMBERS BY COUNTRY\n"
                "8 - TOTAL SALES BY STORE TYPE IN GERMANY\n"
                "9 - AVERAGE TIME TAKEN BETWEEN EACH SALE\n"
            )
            queries = Queries()
            if answer < 1 or answer > 9:
                answer = int(input(f"{text}"))
            number_dash = 70
            if answer == 1:
                number_dash = 26
                queries.get_total_number_stores_by_country(number_dash=number_dash)
            elif answer == 2:
                number_dash = 32
                queries.get_total_number_stores_by_locality(number_dash=number_dash)
            elif answer == 3:
                number_dash = 21
                queries.get_most_sales_per_month(number_dash=number_dash)
            elif answer == 4:
                number_dash = 51
                queries.get_count_sales_by_on_off_line(number_dash=number_dash)
            elif answer == 5:
                number_dash = 47
                queries.get_total_sales_by_store_type(number_dash=number_dash)
            elif answer == 6:
                number_dash = 26
                queries.get_most_sales_per_month_and_year(number_dash=number_dash)
            elif answer == 7:
                number_dash = 30
                queries.get_total_staff_numbers_by_country(number_dash=number_dash)
            elif answer == 8:
                number_dash = 40
                queries.get_total_sales_by_store_type_in_germany(
                    number_dash=number_dash
                )
            elif answer == 9:
                number_dash = 87
                queries.get_average_time_taken_between_each_sale(
                    number_dash=number_dash
                )

            print(f"-" * number_dash)
            answer = input(
                "\nTo stop, insert 'n'. Otherwise, insert any another character:\n"
                "(If is a number between 1 and 9, the query will be executed automatically)\n"
            )
            if answer == "n":
                answer = FINISH
            elif int(answer) > 0 and int(answer) < 10:
                answer = int(answer)
        except ValueError as e:
            print(f"WARNING-> INVALID NUMBER!\n")
            answer = START
