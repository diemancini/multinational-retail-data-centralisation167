# from datetime import datetime
# import uuid
# import re

import pandas as pd
from pandas import DataFrame

from sqlalchemy import (
    Boolean,
    Date,
    Float,
    Engine,
    Integer,
    NVARCHAR,
    SmallInteger,
    UUID,
    VARCHAR,
)
from sqlalchemy import BigInteger, BIGINT
from sqlalchemy import types

from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from base_data_cleaner import BaseDataCleaner


class DataCleaning(BaseDataCleaner):

    def clean_user_data(self, table_name: str) -> DataFrame:
        """
        Clean the user data.

        Parameters:
            - df: Dataframe
        """
        data_extractor: DataExtractor = DataExtractor()
        df: DataFrame = data_extractor.read_rds_table(DatabaseConnector(), table_name)
        for index, row in df.iterrows():
            df.at[index, "first_name"] = self.clean_strings(row["first_name"])
            df.at[index, "last_name"] = self.clean_strings(row["last_name"])
            df.at[index, "company"] = self.clean_strings_with_numbers(row["company"])
            df.at[index, "address"] = self.clean_strings_with_numbers(row["address"])
            df.at[index, "date_of_birth"] = self.clean_date(row["date_of_birth"])
            df.at[index, "join_date"] = self.clean_date(row["join_date"])
            df.at[index, "email_address"] = self.clean_email(row["email_address"])
            df.at[index, "country"] = self.clean_country(
                row["country"], row["country_code"]
            )
            df.at[index, "country_code"] = self.clean_country_code(
                row["country"], row["country_code"]
            )
            df.at[index, "phone_number"] = self.clean_phone_number(row["phone_number"])
            df.at[index, "user_uuid"] = self.clean_uuid(row["user_uuid"])

        df = df.convert_dtypes()

        return df

    def clean_card_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the card data.

        Parameters:
            - df: Dataframe
        """

        for index, row in df.iterrows():
            df.at[index, "card_number"] = self.clean_integer_number(row["card_number"])
            df.at[index, "expiry_date"] = self.clean_expiry_date(row["expiry_date"])
            # Check if the card provider is not in the valid card providers listed names.
            df.at[index, "card_provider"] = self.clean_card_provider(
                row["card_provider"]
            )
            # If there is error in the date pattern, try to convert the date in right format (YYYY-mm-dd) or asign None.
            df.at[index, "date_payment_confirmed"] = self.clean_date(
                row["date_payment_confirmed"]
            )
            # print(df.columns.to_list())
            if df.at[index, "card_number"] is None:
                # print(f"-" * 30)
                # print(f"The row with index {row['index']} will be deleted!")
                # print(row)
                df.drop(index, inplace=True)

        df = df.convert_dtypes()
        df["date_payment_confirmed"] = df["date_payment_confirmed"].astype(
            "datetime64[ns]"
        )

        return df

    def clean_store_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the store data.

        Parameters:
            - df: Dataframe
        """
        # for index, row in df.iterrows():
        #     print(df.at[index, "lat"])
        df = df.drop(columns=["lat"])
        # df = df.droplevel("level_0")
        df = self.sort_store_columns(df)

        for index, row in df.iterrows():
            df.at[index, "staff_numbers"] = self.clean_integer_number(
                row["staff_numbers"]
            )
            df.at[index, "address"] = self.clean_strings_with_numbers(row["address"])
            df.at[index, "locality"] = self.clean_strings(row["locality"])
            df.at[index, "store_type"] = self.clean_store_type(row["store_type"])
            df.at[index, "country_code"] = self.clean_country_code(
                country_code=row["country_code"]
            )
            df.at[index, "longitude"] = self.clean_float_number(row["longitude"])
            df.at[index, "latitude"] = self.clean_float_number(row["latitude"])
            df.at[index, "store_code"] = self.clean_store_code(row["store_code"])
            df.at[index, "opening_date"] = self.clean_date(row["opening_date"])
            df.at[index, "continent"] = self.clean_continent(row["continent"])

            if df.at[index, "store_code"] is None:
                print(f"The row with index {row['index']} will be deleted!")
                df.drop(index, inplace=True)

        # print(len(df.index))
        return df

    def convert_product_weights(self, df: DataFrame) -> DataFrame:
        """
        Convert all the weights to a KG.

        Parameters:
            - df: Dataframe
        """
        for index, row in df.iterrows():
            df.at[index, "weight"] = self.clean_weight(row["weight"])

        return df

    def clean_products_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the products data.

        Parameters:
            - df: Dataframe
        """
        print(df.columns.to_list())
        df.rename(columns={"EAN": "ean"}, inplace=True)
        df.reset_index(inplace=True)
        for index, row in df.iterrows():
            df.at[index, "product_name"] = self.clean_product_name(row["product_name"])
            df.at[index, "product_price"] = self.clean_product_price(
                row["product_price"]
            )
            df.at[index, "date_added"] = self.clean_date(row["date_added"])
            df.at[index, "uuid"] = self.clean_uuid(row["uuid"])
            df.at[index, "removed"] = self.clean_removed(row["removed"])
            df.at[index, "category"] = self.clean_product_category(row["category"])
            df.at[index, "ean"] = self.clean_ean(row["ean"])
            df.at[index, "product_code"] = self.clean_product_code(row["product_code"])

            if df.at[index, "product_code"] is None:
                print(f"The row with index {row['index']} will be deleted!")
                df.drop(index, inplace=True)

        return df

    def clean_orders_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the orders data.

        Parameters:
            - df: Dataframe
        """
        df = df.drop(["level_0", "first_name", "last_name", "1"], axis=1)
        for index, row in df.iterrows():
            df.at[index, "date_uuid"] = self.clean_uuid(row["date_uuid"])
            df.at[index, "user_uuid"] = self.clean_uuid(row["user_uuid"])
            df.at[index, "card_number"] = self.clean_integer_number(row["card_number"])
            df.at[index, "store_code"] = self.clean_store_code(row["store_code"])
            df.at[index, "store_code"] = self.clean_store_code(row["store_code"])
            df.at[index, "product_code"] = self.clean_product_code(row["product_code"])
            df.at[index, "product_quantity"] = self.clean_integer_number(
                row["product_quantity"]
            )

        return df

    def clean_date_details(self, df: DataFrame) -> DataFrame:
        """
        Clean the date details data.

        Parameters:
            - df: Dataframe
        """
        for index, row in df.iterrows():
            df.at[index, "date_uuid"] = self.clean_uuid(row["date_uuid"])
            df.at[index, "time_period"] = self.clean_time_period(row["time_period"])
            df.at[index, "timestamp"] = self.clean_timestamp(row["timestamp"])
            df.at[index, "day"] = self.clean_day(row["day"])
            df.at[index, "month"] = self.clean_month(row["month"])
            df.at[index, "year"] = self.clean_year(row["year"])

        return df

    def add_data_into_weight_class(self, db: DatabaseConnector) -> None:
        """
        Parameters:
            - db: DatabaseConnector

        OBS:
            # +--------------------------+-------------------+
            # | weight_class VARCHAR(?)  | weight range(kg)  |
            # +--------------------------+-------------------+
            # | Light                    | < 2               |
            # | Mid_Sized                | >= 2 - < 40       |
            # | Heavy                    | >= 40 - < 140     |
            # | Truck_Required           | => 140            |
            # +----------------------------+-----------------+
        """
        LIGHT: str = "Light"
        MID_SIZED: str = "Mid_Sized"
        HEAVY: str = "Heavy"
        TRUCK_REQUIRED = "Truck_Required"

        select_query = "SELECT index, weight FROM dim_products;"
        df: DataFrame = db.select_db(select_query)
        # df: DataFrame = pd.read_sql(select_query, con=engine)
        for index, row in df.iterrows():
            weight = row["weight"]
            weight_class = LIGHT
            if weight >= 2 and weight < 40:
                weight_class = MID_SIZED
            elif weight >= 40 and weight < 140:
                weight_class = HEAVY

            elif weight >= 140:
                weight_class = TRUCK_REQUIRED

            insert_query = f"UPDATE dim_products set weight_class='{weight_class}' WHERE index={row['index']}"
            db.insert_db(insert_query)

    def remove_character_from_column_value(
        self, table_name: str, column_name: str, value: str, new_value: str
    ):
        """
        Parameters:
            - table_name: string
            - column_name: string
            - value: string
            - new_value: string
        """
        # UPDATE posts
        # SET url = REPLACE(url, 'http','https');
        select_query = f"SELECT index, {column_name} FROM {table_name};"
        update_query = f"UPDATE {table_name} set {column_name} = REPLACE({column_name}, '{value}', '{new_value}')"
        db.insert_db(update_query)
        # df: DataFrame = db.select_db(select_query)
        # for index, row in df.iterrows():

    def convert_column_value_to_boolean(
        self, table_name: str, column_name: str, value: str
    ):
        """
        Parameters:
            - table_name: string
            - column_name: string
            - value: string -> String value that will be used for validating TRUE statement.
        """
        select_query = f"SELECT index, {column_name} FROM {table_name};"
        df: DataFrame = db.select_db(select_query)
        for index, row in df.iterrows():
            is_true = True
            if row[column_name] != value:
                is_true = False
            update_query = f"UPDATE {table_name} set {column_name}={is_true} WHERE index={row['index']}"
            db.insert_db(update_query)


if __name__ == "__main__":
    data_cleaning = DataCleaning()
    data_extractor = DataExtractor()
    db = DatabaseConnector()

    # TASK 9 (MILESTONE 3)
    """
    With the primary keys created in the tables prefixed with dim we can now create the foreign keys in the orders_table to reference the primary keys in the other tables.
    Use SQL to create those foreign key constraints that reference the primary keys of the other table.
    This makes the star-based database schema complete.
    """
    # OK
    # db.add_foreign_key(
    #     child_table="orders_table",
    #     parent_table="dim_card_details",
    #     fk_column="card_number",
    #     pk_column="card_number",
    # )
    # OK
    # db.add_foreign_key(
    #     child_table="orders_table",
    #     parent_table="dim_store_details",
    #     fk_column="store_code",
    #     pk_column="store_code",
    # )
    # OK
    # db.add_foreign_key(
    #     child_table="orders_table",
    #     parent_table="dim_users",
    #     fk_column="user_uuid",
    #     pk_column="user_uuid",
    # )
    # OK
    # db.add_foreign_key(
    #     child_table="orders_table",
    #     parent_table="dim_products",
    #     fk_column="product_code",
    #     pk_column="product_code",
    # )
    # OK
    # db.add_foreign_key(
    #     child_table="orders_table",
    #     parent_table="dim_date_times",
    #     fk_column="date_uuid",
    #     pk_column="date_uuid",
    # )

    # TASK 8 (MILESTONE 3)
    """
    Now that the tables have the appropriate data types we can begin adding the primary keys to each of the tables prefixed with dim.
    Each table will serve the orders_table which will be the single source of truth for our orders.
    Check the column header of the orders_table you will see all but one of the columns exist in one of our tables prefixed with dim.
    We need to update the columns in the dim tables with a primary key that matches the same column in the orders_table.
    Using SQL, update the respective columns as primary key columns.
    """
    # db.add_primary_key_on_column("dim_card_details", "card_number")
    # db.add_primary_key_on_column("dim_users", "user_uuid")
    # db.add_primary_key_on_column("dim_store_details", "store_code")
    # db.add_primary_key_on_column("dim_products", "product_code")
    # db.add_primary_key_on_column("dim_date_times", "date_uuid")

    # TASK 7 (MILESTONE 3)
    # +------------------------+-------------------+--------------------+
    # |    dim_card_details    | current data type | required data type |
    # +------------------------+-------------------+--------------------+
    # | card_number            | TEXT              | VARCHAR(?)         |
    # | expiry_date            | TEXT              | VARCHAR(?)         |
    # | date_payment_confirmed | TEXT              | DATE               |
    # +------------------------+-------------------+--------------------+
    # columns = [
    #     ["card_number", VARCHAR(30)],
    #     ["expiry_date", VARCHAR(5)],
    #     ["date_payment_confirmed", Date],
    # ]
    # db.update_datatypes_db("dim_card_details", columns)

    # TASK 6 (MILESTONE 3)
    # +-----------------+-------------------+--------------------+
    # | dim_date_times  | current data type | required data type |
    # +-----------------+-------------------+--------------------+
    # | month           | TEXT              | VARCHAR(?)         |
    # | year            | TEXT              | VARCHAR(?)         |
    # | day             | TEXT              | VARCHAR(?)         |
    # | time_period     | TEXT              | VARCHAR(?)         |
    # | date_uuid       | TEXT              | UUID               |
    # +-----------------+-------------------+--------------------+
    # columns = [
    #     ["month", VARCHAR(15)],
    #     ["year", VARCHAR(5)],
    #     ["day", VARCHAR(10)],
    #     ["time_period", VARCHAR(30)],
    #     ["date_uuid", UUID],
    # ]
    # db.update_datatypes_db("dim_date_times", columns)

    # TASK 5 (MILESTONE 3)
    # +-----------------+--------------------+--------------------+
    # |  dim_products   | current data type  | required data type |
    # +-----------------+--------------------+--------------------+
    # | product_price   | TEXT               | FLOAT              |
    # | weight          | TEXT               | FLOAT              |
    # | EAN             | TEXT               | VARCHAR(?)         |
    # | product_code    | TEXT               | VARCHAR(?)         |
    # | date_added      | TEXT               | DATE               |
    # | uuid            | TEXT               | UUID               |
    # | still_available | TEXT               | BOOL               |
    # | weight_class    | TEXT               | VARCHAR(?)         |
    # +-----------------+--------------------+--------------------+
    # df = data_extractor.extract_from_s3(
    #     "data-handling-public", "products.csv", "data/products.csv"
    # )
    # print(df.columns.to_list())
    # db.rename_column_name(
    #     table_name="dim_products",
    #     column_name="removed",
    #     new_column_name="still_available",
    # )
    # data_cleaning.convert_column_value_to_boolean(
    #     table_name="dim_products",
    #     column_name="still_available",
    #     value="Still_avaliable",
    # )
    # data_cleaning.remove_character_from_column_value(
    #     table_name="dim_products", column_name="product_price", value="£", new_value=""
    # )
    # columns = [
    #     ["product_price", Float],
    #     ["weight", Float],
    #     ["ean", VARCHAR(15)],
    #     ["product_code", VARCHAR(12)],
    #     ["date_added", Date],
    #     ["uuid", UUID],
    #     ["still_available", Boolean],
    #     ["weight_class", VARCHAR(20)],
    # ]
    # db.update_datatypes_db("dim_products", columns)

    # TASK 4 (MILESTONE 3)
    # +--------------------------+-------------------+
    # | weight_class VARCHAR(?)  | weight range(kg)  |
    # +--------------------------+-------------------+
    # | Light                    | < 2               |
    # | Mid_Sized                | >= 2 - < 40       |
    # | Heavy                    | >= 40 - < 140     |
    # | Truck_Required           | => 140            |
    # +----------------------------+-----------------+
    # columns = [["weight_class", VARCHAR(20)]]
    # db.create_new_column("dim_products", columns=columns)
    # data_cleaning.add_data_into_weight_class(db)

    # TASK 3 (MILESTONE 3)
    # number_of_stores = data_extractor.list_number_of_stores()
    # df = data_extractor.retrieve_stores_data(100)
    # df_cleaned = data_cleaning.clean_store_data(df)
    # columns_type = [
    #     ["address", VARCHAR(255)],
    #     ["longitude", Float],
    #     ["latitude", Float],
    #     ["locality", VARCHAR(255)],
    #     ["store_code", VARCHAR(13)],
    #     ["staff_numbers", SmallInteger],
    #     ["opening_date", Date],
    #     ["store_type", VARCHAR(255)],
    #     ["country_code", VARCHAR(2)],
    #     ["continent", VARCHAR(255)],
    # ]
    # # db.upload_to_db(df_cleaned, "dim_store_details", columns_type, is_constraints=True)
    # db.update_datatypes_db("dim_store_details", columns_type)

    # TASK 8
    # url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    # df = data_extractor.extract_json_from_s3(url)
    # df = data_cleaning.clean_date_details(df)
    # columns_type = {"index": BigInteger}
    # db.upload_to_db(df, "dim_date_times", column_types=columns_type)

    # TASK 7
    # tables = data_extractor.list_db_tables()
    # df = data_extractor.read_rds_table(DatabaseConnector(), tables[-1])
    # df = data_cleaning.clean_orders_data(df)
    # columns_types = {
    #     "date_uuid": UUID,
    #     "user_uuid": UUID,
    #     "card_number": VARCHAR(length=20),
    #     "store_code": VARCHAR(length=20),
    #     "product_code": VARCHAR(length=20),
    #     "product_quantity": SmallInteger,
    # }
    # db.upload_to_db(df, "orders_table", column_types=columns_types)

    # TASK 6
    # df = data_extractor.extract_from_s3(
    #     "data-handling-public", "products.csv", "data/products.csv"
    # )
    # df = data_cleaning.convert_product_weights(df)
    # df = data_cleaning.clean_products_data(df)
    # columns_type = {"weight": Float}
    # db.upload_to_db(df, "dim_products", column_types=columns_type)

    # TASK 5
    # number_of_stores = data_extractor.list_number_of_stores()
    # df = data_extractor.retrieve_stores_data(number_of_stores)
    # df_cleaned = data_cleaning.clean_store_data(df)
    # columns_type = [
    #     ["index", BIGINT, " PRIMARY KEY NOT NULL"],
    #     ["address", VARCHAR(255)],
    #     ["longitude", Float],
    #     ["latitude", Float],
    #     ["locality", VARCHAR(255)],
    #     ["store_code", VARCHAR(13)],
    #     ["staff_numbers", SmallInteger],
    #     ["opening_date", Date],
    #     ["store_type", VARCHAR(255)],
    #     ["country_code", VARCHAR(2)],
    #     ["continent", VARCHAR(255)],
    # ]
    # db.upload_to_db(
    #     df_cleaned,
    #     "dim_store_details",
    #     columns_type,
    #     is_constraints=True,
    #     drop_table=True,
    # )

    # TASK 4
    # pdf_path = (
    #     "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    # )
    # df = data_extractor.retrieve_pdf_data(pdf_path)
    # df = data_cleaning.clean_card_data(df)
    # # [
    # #     print(row["card_number"])
    # #     for index, row in df.iterrows()
    # #     if row["card_number"] == 4537509987455280000
    # # ]
    # columns_types = {"card_number": BigInteger, "date_payment_confirmed": Date}
    # db.upload_to_db(df, "dim_card_details", columns_types)

    # TASK 3
    # df = data_cleaning.clean_user_data("legacy_users")
    # print(df.columns.to_list())
    # columns_types = {
    #     "first_name": VARCHAR(length=255),
    #     "last_name": VARCHAR(length=255),
    #     "country_code": VARCHAR(length=2),
    #     "user_uuid": UUID,
    #     "date_of_birth": Date,
    #     "join_date": Date,
    # }
    # db.upload_to_db(df, "dim_users", columns_types)
