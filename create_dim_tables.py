from database_manager.data_cleaning import DataCleaning
from database_manager.database_utils import DatabaseConnector
from database_manager.data_extraction import DataExtractor


from pandas import DataFrame

from sqlalchemy import (
    BIGINT,
    BigInteger,
    Boolean,
    Date,
    Float,
    SmallInteger,
    UUID,
    VARCHAR,
    exc,
)


class CreateDimTables(DataCleaning, DatabaseConnector, DataExtractor):

    def create_orders_table(func):
        """
        **** DECORATOR FUNCTION ****
        Any of upload dim functions must have be executed only if the orders table
        was already created in database. This decorator 'force' the creation of
        the orders table.
        """

        def wrapper(self):
            CreateDimTables.upload_orders_table(self)
            print(f"EXECUTING {func.__name__}...")
            func(self)

        return wrapper

    def upload_orders_table(self) -> bool:
        """
        This function will execute the tasks implemented below:
            TASK 7 in MILESTONE 2

        In order to run any others upload dim tables functions of this class(upload_dim_products, for instance),
        this function must to be called first(just one time).
        """
        # TASK 7 (MILESTONE 2)
        try:
            select_query: str = f"SELECT * FROM orders_table LIMIT 2"
            df: DataFrame = self.select_db(select_query)
            if not df.empty:
                print("-" * 30)
                print(f"THE 'ORDERS TABLE' WAS ALREADY CREATED BEFORE!")
        except exc.ProgrammingError as e:
            print(f"CREATING ORDERS TABLE.. :\n{e}")
            tables = self.list_db_tables()
            df = self.read_rds_table(DatabaseConnector(), tables[-1])
            df = self.clean_orders_data(df)
            columns_types = {
                "date_uuid": UUID,
                "user_uuid": UUID,
                "card_number": VARCHAR(length=20),
                "store_code": VARCHAR(length=20),
                "product_code": VARCHAR(length=20),
                "product_quantity": SmallInteger,
            }
            self.upload_to_db(df, "orders_table", column_types=columns_types)

    @create_orders_table
    def upload_dim_users(self):
        """
        This function will execute the tasks implemented below:
            TASK 3 in MILESTONE 2
            TASK 2 in MILESTONE 3
            TASK 8 in MILESTONE 3
            TASK 9 in MILESTONE 3
        """
        df = self.clean_user_data("legacy_users")
        # TASK 3 in MILESTONE 2 and TASK 2 in MILESTONE 3
        columns_types = {
            "first_name": VARCHAR(length=255),
            "last_name": VARCHAR(length=255),
            "country_code": VARCHAR(length=2),
            "user_uuid": UUID,
            "date_of_birth": Date,
            "join_date": Date,
        }
        self.upload_to_db(df, "dim_users", columns_types)
        # TASK 8 (MILESTONE 3)
        self.add_primary_key_on_column("dim_users", "user_uuid")
        # TASK 9 (MILESTONE 3)
        self.add_foreign_key(
            child_table="orders_table",
            parent_table="dim_users",
            fk_column="user_uuid",
            pk_column="user_uuid",
        )

    @create_orders_table
    def upload_dim_card_details(self):
        """
        This function will execute the tasks implemented below:
            TASK 4 in MILESTONE 2
            TASK 7 in MILESTONE 3
            TASK 8 in MILESTONE 3
            TASK 9 in MILESTONE 3
        """
        # TASK 4 in MILESTONE 2
        pdf_path = (
            "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        )
        df = self.retrieve_pdf_data(pdf_path)
        df = self.clean_card_data(df)
        columns_types = {"card_number": BigInteger, "date_payment_confirmed": Date}
        self.upload_to_db(df, "dim_card_details", columns_types)
        # TASK 7 (MILESTONE 3)
        columns = [
            ["index", BIGINT],
            ["card_number", VARCHAR(30)],
            ["expiry_date", VARCHAR(5)],
            ["date_payment_confirmed", Date],
        ]
        self.update_datatypes_db("dim_card_details", columns)
        # TASK 8 (MILESTONE 3)
        self.add_primary_key_on_column("dim_card_details", "card_number")
        # TASK 9 (MILESTONE 3)
        self.add_foreign_key(
            child_table="orders_table",
            parent_table="dim_card_details",
            fk_column="card_number",
            pk_column="card_number",
        )

    @create_orders_table
    def upload_dim_store_details(self):
        """
        This function will execute the tasks implemented below:
            TASK 5 in MILESTONE 2
            TASK 3 in MILESTONE 3
            TASK 8 in MILESTONE 3
            TASK 9 in MILESTONE 3
        """
        # TASK 5 (MILESTONE 3)
        number_of_stores = self.list_number_of_stores()
        df = self.retrieve_stores_data(number_of_stores)
        df_cleaned = self.clean_store_data(df)
        columns = [
            ["index", BIGINT],
            ["address", VARCHAR(255)],
            ["longitude", Float],
            ["latitude", Float],
            ["locality", VARCHAR(255)],
            ["store_code", VARCHAR(13)],
            ["staff_numbers", SmallInteger],
            ["opening_date", Date],
            ["store_type", VARCHAR(255)],
            ["country_code", VARCHAR(2)],
            ["continent", VARCHAR(255)],
        ]
        self.upload_to_db(
            df_cleaned,
            "dim_store_details",
            column_types=columns,
            using_query=True,
        )
        # TASK 3 (MILESTONE 3)
        # This part of the code doesn't need to be run, because the task
        # above already done the job.
        columns_type = [
            ["address", VARCHAR(255)],
            ["longitude", Float],
            ["latitude", Float],
            ["locality", VARCHAR(255)],
            ["store_code", VARCHAR(13)],
            ["staff_numbers", SmallInteger],
            ["opening_date", Date],
            ["store_type", VARCHAR(255)],
            ["country_code", VARCHAR(2)],
            ["continent", VARCHAR(255)],
        ]
        self.update_datatypes_db("dim_store_details", columns_type)
        # TASK 8 (MILESTONE 3)
        self.add_primary_key_on_column("dim_store_details", "store_code")
        # TASK 9 (MILESTONE 3)
        self.add_foreign_key(
            child_table="orders_table",
            parent_table="dim_store_details",
            fk_column="store_code",
            pk_column="store_code",
        )

    @create_orders_table
    def upload_dim_products(self):
        """
        This function will execute the tasks implemented below:
            TASK 6 in MILESTONE 2
            TASK 4 in MILESTONE 3
            TASK 5 in MILESTONE 3
            TASK 8 in MILESTONE 3
            TASK 9 in MILESTONE 3
        """
        # TASK 6 (MILESTONE 2)
        df = self.extract_from_s3(
            "data-handling-public", "products.csv", "data/products.csv"
        )
        df = self.convert_product_weights(df)
        df = self.clean_products_data(df)
        columns = [
            ["index", BIGINT],
            ["product_name", VARCHAR(255)],
            ["category", VARCHAR(50)],
            ["product_price", VARCHAR(20)],
            ["weight", Float],
            ["ean", VARCHAR(30)],
            ["product_code", VARCHAR(12)],
            ["date_added", Date],
            ["uuid", UUID],
            ["removed", VARCHAR(30)],
            ["weight_class", VARCHAR(20)],
        ]
        self.upload_to_db(
            df,
            table_name="dim_products",
            column_types=columns,
            using_query=True,
        )
        # TASK 4 (MILESTONE 3)
        columns = [["weight_class", VARCHAR(20)]]
        self.create_new_column("dim_products", columns=columns)
        self.add_data_into_weight_class()
        # TASK 5 (MILESTONE 3)
        df = self.extract_from_s3(
            "data-handling-public", "products.csv", "data/products.csv"
        )
        self.rename_column_name(
            table_name="dim_products",
            column_name="removed",
            new_column_name="still_available",
        )
        self.convert_column_value_to_boolean(
            table_name="dim_products",
            column_name="still_available",
            value="Still_avaliable",
        )
        self.remove_character_from_column_value(
            table_name="dim_products",
            column_name="product_price",
            value="£",
            new_value="",
        )
        columns = [
            ["product_price", Float],
            ["weight", Float],
            ["ean", VARCHAR(15)],
            ["product_code", VARCHAR(12)],
            ["date_added", Date],
            ["uuid", UUID],
            ["still_available", Boolean],
            ["weight_class", VARCHAR(20)],
        ]
        self.update_datatypes_db("dim_products", columns)
        # TASK 8 (MILESTONE 3)
        self.add_primary_key_on_column("dim_products", "product_code")
        # TASK 9 (MILESTONE 3)
        self.add_foreign_key(
            child_table="orders_table",
            parent_table="dim_products",
            fk_column="product_code",
            pk_column="product_code",
        )

    @create_orders_table
    def upload_dim_date_times(self):
        """
        This function will execute the tasks implemented below:
            TASK 8 in MILESTONE 2
            TASK 6 in MILESTONE 3
            TASK 8 in MILESTONE 3
            TASK 9 in MILESTONE 3
        """
        # TASK 8 (MILESTONE 2)
        url = (
            "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        )
        df = self.extract_json_from_s3(url)
        df = self.clean_date_details(df)
        columns_type = {"index": BigInteger}
        self.upload_to_db(df, table_name="dim_date_times", column_types=columns_type)
        # TASK 6 (MILESTONE 3)
        columns = [
            ["month", VARCHAR(15)],
            ["year", VARCHAR(5)],
            ["day", VARCHAR(10)],
            ["time_period", VARCHAR(30)],
            ["date_uuid", UUID],
        ]
        self.update_datatypes_db("dim_date_times", columns)
        # TASK 8 (MILESTONE 3)
        self.add_primary_key_on_column("dim_date_times", "date_uuid")
        # TASK 9 (MILESTONE 3)
        self.add_foreign_key(
            child_table="orders_table",
            parent_table="dim_date_times",
            fk_column="date_uuid",
            pk_column="date_uuid",
        )


if __name__ == "__main__":
    try:
        text = (
            ""
            "Please, which dim table you want to be created?\n\n"
            "0 - ALL\n"
            "1 - DIM_USERS\n"
            "2 - DIM_CARD_DETAILS\n"
            "3 - DIM_STORE_DETAILS\n"
            "4 - DIM_PRODUCTS\n"
            "5 - DIM_DATE_TIMES\n\n"
        )
        create_dim_tables = CreateDimTables()
        answer = int(input(f"{text}"))
        if answer == 0:
            print(f"Creating all existing tables...")
            create_dim_tables.upload_dim_users()
            create_dim_tables.upload_dim_card_details()
            create_dim_tables.upload_dim_store_details()
            create_dim_tables.upload_dim_products()
            create_dim_tables.upload_dim_date_times()

        elif answer == 1:
            create_dim_tables.upload_dim_users()
        elif answer == 2:
            create_dim_tables.upload_dim_card_details()
        elif answer == 3:
            create_dim_tables.upload_dim_store_details()
        elif answer == 4:
            create_dim_tables.upload_dim_products()
        elif answer == 5:
            create_dim_tables.upload_dim_date_times()
        else:
            print(f"INVALID OPTION. Select one of the options above.\n")

    except ValueError as e:
        print(f"ERROR -> INVALID NUMBER{e}\n")
