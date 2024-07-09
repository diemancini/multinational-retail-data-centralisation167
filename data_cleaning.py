from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from base_data_cleaner import BaseDataCleaner

from pandas import DataFrame


class DataCleaning(BaseDataCleaner, DatabaseConnector):

    def clean_user_data(self, table_name: str) -> DataFrame:
        """
        Clean the user data.

        Parameters:
            - df: Dataframe
        """
        data_extractor: DataExtractor = DataExtractor()
        df: DataFrame = data_extractor.read_rds_table(DatabaseConnector(), table_name)
        for index, row in df.iterrows():
            df.at[index, self._INDEX_FIRST_NAME] = self.clean_strings(
                row[self._INDEX_FIRST_NAME]
            )
            df.at[index, self._INDEX_LAST_NAME] = self.clean_strings(
                row[self._INDEX_LAST_NAME]
            )
            df.at[index, self._INDEX_COMPANY] = self.clean_strings_with_numbers(
                row[self._INDEX_COMPANY]
            )
            df.at[index, self._INDEX_ADDRESS] = self.clean_strings_with_numbers(
                row[self._INDEX_ADDRESS]
            )
            df.at[index, self._INDEX_DATE_OF_BIRTH] = self.clean_date(
                row[self._INDEX_DATE_OF_BIRTH]
            )
            df.at[index, self._INDEX_JOIN_DATE] = self.clean_date(
                row[self._INDEX_JOIN_DATE]
            )
            df.at[index, self._INDEX_EMAIL_ADDRESS] = self.clean_email(
                row[self._INDEX_EMAIL_ADDRESS]
            )
            df.at[index, self._INDEX_COUNTRY] = self.clean_country(
                row[self._INDEX_COUNTRY], row[self._INDEX_COUNTRY_CODE]
            )
            df.at[index, self._INDEX_COUNTRY_CODE] = self.clean_country_code(
                row[self._INDEX_COUNTRY], row[self._INDEX_COUNTRY_CODE]
            )
            df.at[index, self._INDEX_PHONE_NUMBER] = self.clean_phone_number(
                row[self._INDEX_PHONE_NUMBER]
            )
            df.at[index, self._INDEX_USER_UUID] = self.clean_uuid(
                row[self._INDEX_USER_UUID]
            )

        df = df.convert_dtypes()

        return df

    def clean_card_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the card data.

        Parameters:
            - df: Dataframe
        """

        for index, row in df.iterrows():
            df.at[index, self._INDEX_CARD_NUMBER] = self.clean_integer_number(
                row[self._INDEX_CARD_NUMBER]
            )
            df.at[index, self._INDEX_EXPIRY_DATE] = self.clean_expiry_date(
                row[self._INDEX_EXPIRY_DATE]
            )
            # Check if the card provider is not in the valid card providers listed names.
            df.at[index, self._INDEX_CARD_PROVIDER] = self.clean_card_provider(
                row[self._INDEX_CARD_PROVIDER]
            )
            # If there is error in the date pattern, try to convert the date in right format (YYYY-mm-dd) or asign None.
            df.at[index, self._INDEX_DATE_PAYMENT_CONFIRMED] = self.clean_date(
                row[self._INDEX_DATE_PAYMENT_CONFIRMED]
            )
            # print(df.columns.to_list())
            if df.at[index, self._INDEX_CARD_NUMBER] is None:
                df.drop(index, inplace=True)

        df = df.convert_dtypes()
        df[self._INDEX_DATE_PAYMENT_CONFIRMED] = df[
            self._INDEX_DATE_PAYMENT_CONFIRMED
        ].astype("datetime64[ns]")

        return df

    def clean_store_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the store data.

        Parameters:
            - df: Dataframe
        """
        # for index, row in df.iterrows():
        #     print(df.at[index, self._INDEX_LAT])
        df = df.drop(columns=[self._INDEX_LAT])
        df = self.sort_store_columns(df)

        for index, row in df.iterrows():
            df.at[index, self._INDEX_STAFF_NUMBERS] = self.clean_integer_number(
                row[self._INDEX_STAFF_NUMBERS]
            )
            df.at[index, self._INDEX_ADDRESS] = self.clean_strings_with_numbers(
                row[self._INDEX_ADDRESS]
            )
            df.at[index, self._INDEX_LOCALITY] = self.clean_strings(
                row[self._INDEX_LOCALITY]
            )
            df.at[index, self._INDEX_STORE_TYPE] = self.clean_store_type(
                row[self._INDEX_STORE_TYPE]
            )
            df.at[index, self._INDEX_COUNTRY_CODE] = self.clean_country_code(
                country_code=row[self._INDEX_COUNTRY_CODE]
            )
            df.at[index, self._INDEX_LONGITUDE] = self.clean_float_number(
                row[self._INDEX_LONGITUDE]
            )
            df.at[index, self._INDEX_LATITUDE] = self.clean_float_number(
                row[self._INDEX_LATITUDE]
            )
            df.at[index, self._INDEX_STORE_CODE] = self.clean_store_code(
                row[self._INDEX_STORE_CODE]
            )
            df.at[index, self._INDEX_OPENING_DATE] = self.clean_date(
                row[self._INDEX_OPENING_DATE]
            )
            df.at[index, self._INDEX_CONTINENT] = self.clean_continent(
                row[self._INDEX_CONTINENT]
            )

            if df.at[index, self._INDEX_STORE_CODE] is None:
                print(f"The row with index {row[self._INDEX]} will be deleted!")
                df.drop(index, inplace=True)

        return df

    def convert_product_weights(self, df: DataFrame) -> DataFrame:
        """
        Convert all the weights to a KG.

        Parameters:
            - df: Dataframe
        """
        for index, row in df.iterrows():
            df.at[index, self._INDEX_WEIGHT] = self.clean_weight(
                row[self._INDEX_WEIGHT]
            )

        return df

    def clean_products_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the products data.

        Parameters:
            - df: Dataframe
        """
        df.rename(columns={"EAN": "ean"}, inplace=True)
        df.reset_index(inplace=True)
        for index, row in df.iterrows():
            df.at[index, self._INDEX_PRODUCT_NAME] = self.clean_product_name(
                row[self._INDEX_PRODUCT_NAME]
            )
            df.at[index, self._INDEX_PRODUCT_PRICE] = self.clean_product_price(
                row[self._INDEX_PRODUCT_PRICE]
            )
            df.at[index, self._INDEX_DATE_ADDED] = self.clean_date(
                row[self._INDEX_DATE_ADDED]
            )
            df.at[index, self._INDEX_UUID] = self.clean_uuid(row[self._INDEX_UUID])
            df.at[index, self._INDEX_REMOVED] = self.clean_removed(
                row[self._INDEX_REMOVED]
            )
            df.at[index, self._INDEX_CATEGORY] = self.clean_product_category(
                row[self._INDEX_CATEGORY]
            )
            df.at[index, self._INDEX_EAN] = self.clean_ean(row[self._INDEX_EAN])
            df.at[index, self._INDEX_PRODUCT_CODE] = self.clean_product_code(
                row[self._INDEX_PRODUCT_CODE]
            )

            if df.at[index, self._INDEX_PRODUCT_CODE] is None:
                print(f"The row with index {row[self._INDEX]} will be deleted!")
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
            df.at[index, self._INDEX_DATE_UUID] = self.clean_uuid(
                row[self._INDEX_DATE_UUID]
            )
            df.at[index, self._INDEX_USER_UUID] = self.clean_uuid(
                row[self._INDEX_USER_UUID]
            )
            df.at[index, self._INDEX_CARD_NUMBER] = self.clean_integer_number(
                row[self._INDEX_CARD_NUMBER]
            )
            df.at[index, self._INDEX_STORE_CODE] = self.clean_store_code(
                row[self._INDEX_STORE_CODE]
            )
            df.at[index, self._INDEX_STORE_CODE] = self.clean_store_code(
                row[self._INDEX_STORE_CODE]
            )
            df.at[index, self._INDEX_PRODUCT_CODE] = self.clean_product_code(
                row[self._INDEX_PRODUCT_CODE]
            )
            df.at[index, self._INDEX_PRODUCT_QUANTITY] = self.clean_integer_number(
                row[self._INDEX_PRODUCT_QUANTITY]
            )

        return df

    def clean_date_details(self, df: DataFrame) -> DataFrame:
        """
        Clean the date details data.

        Parameters:
            - df: Dataframe
        """
        df.reset_index(inplace=True)
        for index, row in df.iterrows():
            df.at[index, self._INDEX_DATE_UUID] = self.clean_uuid(
                row[self._INDEX_DATE_UUID]
            )
            df.at[index, self._INDEX_TIME_PERIOD] = self.clean_time_period(
                row[self._INDEX_TIME_PERIOD]
            )
            df.at[index, self._INDEX_TIMESTAMP] = self.clean_timestamp(
                row[self._INDEX_TIMESTAMP]
            )
            df.at[index, self._INDEX_DAY] = self.clean_day(row[self._INDEX_DAY])
            df.at[index, self._INDEX_MONTH] = self.clean_month(row[self._INDEX_MONTH])
            df.at[index, self._INDEX_YEAR] = self.clean_year(row[self._INDEX_YEAR])

        return df

    def add_data_into_weight_class(self) -> None:
        """
        OBS:
            +--------------------------+-------------------+
            | weight_class VARCHAR(?)  | weight range(kg)  |
            +--------------------------+-------------------+
            | Light                    | < 2               |
            | Mid_Sized                | >= 2 - < 40       |
            | Heavy                    | >= 40 - < 140     |
            | Truck_Required           | => 140            |
            +----------------------------+-----------------+
        """
        LIGHT: str = "Light"
        MID_SIZED: str = "Mid_Sized"
        HEAVY: str = "Heavy"
        TRUCK_REQUIRED = "Truck_Required"

        select_query = "SELECT index, weight FROM dim_products;"
        df: DataFrame = self.select_db(select_query)
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

            insert_query = f"UPDATE dim_products set weight_class='{weight_class}' WHERE index={row[self._INDEX]}"
            self.insert_db(insert_query, output=False)

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
        update_query = f"UPDATE {table_name} set {column_name} = REPLACE({column_name}, '{value}', '{new_value}')"
        self.insert_db(update_query)

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
        df: DataFrame = self.select_db(select_query)
        for index, row in df.iterrows():
            is_true = True
            if row[column_name] != value:
                is_true = False
            update_query = f"UPDATE {table_name} set {column_name}={is_true} WHERE index={row[self._INDEX]}"
            self.insert_db(update_query, output=False)
