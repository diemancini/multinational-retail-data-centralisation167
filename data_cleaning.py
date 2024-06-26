from datetime import datetime
import uuid
import re

import pandas as pd
from sqlalchemy import UUID, BigInteger, Integer, Date, Float

from data_extraction import DataExtractor
from database_utils import DatabaseConnector


class DataCleaning:
    __MAP_COUNTRY_CODE = {
        "DE": "Germany",
        "GB": "United Kingdom",
        "US": "United States",
    }
    __COUNTRIES = ["Germany", "United Kingdom", "United States"]
    __CONTINENTS = ["America", "Europe"]
    __CARD_PROVIDERS = [
        "Mastercard",
        "JCB 16 digit",
        "VISA 16 digit",
        "Diners Club / Carte Blanche",
        "American Express",
        "JCB 15 digit",
        "VISA 13 digit",
        "Maestro",
        "Discover",
        "VISA 19 digit",
    ]
    __STORE_TYPE = [
        "Mall Kiosk",
        "Local",
        "Outlet",
        "Super Store",
        "Web Portal",
    ]

    __REMOVED_ITEM = ["Still_avaliable", "Removed"]

    __PRODUCTS_CATEGORY = [
        "sports-and-leisure",
        "diy",
        "pets",
        "toys-and-games",
        "food-and-drink",
        "health-and-beauty",
        "homeware",
    ]

    __TIME_PERIOD = ["Morning", "Evening", "Midday", "Late_Hours"]

    def __clean_strings(self, string):
        if not string or string == "NULL" or string == "N/A":
            # print(f"string: {string}")
            string = None
        return string

    def __check_date_format(self, date, format):
        """
        Check if the date string is in date format.

        Parameters:
            - date: string
            - format: string -> Pattern of date
        """
        is_format = True
        try:
            is_format = bool(datetime.strptime(date, format))
        except ValueError:
            is_format = False
        except TypeError:
            is_format = False

        return is_format

    def __clean_date(self, date):
        """
        Convert datetime fields to %Y-%m-%d format. Otherwise,  or None.

        Parameters:
            - date: string
        """
        if self.__check_date_format(date, "%Y-%m-%d"):
            return pd.to_datetime(date).date()
        elif (
            self.__check_date_format(date, "%B %Y %d")
            or self.__check_date_format(date, "%Y %B %d")
            or self.__check_date_format(date, "%Y/%m/%d")
        ):
            return pd.to_datetime(date).date()
        else:
            return pd.to_datetime("1900-01-01").date()

    def __clean_continet(self, continent):
        new_continent = continent
        if new_continent not in self.__CONTINENTS:
            regex = r"((\w)\2{1,})"
            if re.search(regex, new_continent):
                new_continent = re.sub(regex, "", new_continent)
            else:
                new_continent = None

        return new_continent

    def __clean_email(self, email):
        """
        Clean the email that contains 0, 2 or more '@' character(s).

        Parameters:
            - email: string
        """
        regex = r"^[\w0-9_.+-]+@[\w-]{2,}\.[\w]{2,}[\.\w]{0,}$"
        new_email = email
        if re.search(regex, email) is None:
            at_regex = r"@{2,}"
            if re.search(at_regex, email):
                new_email = re.sub(at_regex, "@", email)
            elif re.search(r"[^@]", email):
                new_email = None

        return new_email

    def __clean_country(self, country, country_code):
        """
        Parameters:

            - country: string
            - country_code: string
        """
        new_country = country
        for key, value in self.__MAP_COUNTRY_CODE.items():
            if key == country_code and country not in self.__COUNTRIES:
                new_country = value
        if country == "NULL":
            new_country = None

        return new_country

    def __clean_country_code(self, country=None, country_code=None):
        """
        Parameters:
            - country: string
            - country_code: string
        """
        new_country_code = country_code
        if country is None:
            try:
                if self.__MAP_COUNTRY_CODE[new_country_code] == country:
                    return new_country_code
            except KeyError:
                for key, value in self.__MAP_COUNTRY_CODE.items():
                    if value == country:
                        new_country_code = key
                new_country_code = None

        else:
            country_codes = [key for key, value in self.__MAP_COUNTRY_CODE]
            if new_country_code not in country_codes:
                new_country_code = None

        return new_country_code

    def __clean_phone_number(self, phone_number):
        """
        Removes any alphabetic and special character (except plus sign) from the phone numbers.

        Parameters:
            - phone_number: string
        """
        new_phone_number = phone_number
        regex = r"[a-zA-Z-\s\-\.\(\)]+"
        if phone_number == "NULL":
            new_phone_number = None
        elif re.search(regex, phone_number) is not None:
            new_phone_number = re.sub(r"[a-zA-Z\(\)\s\-\.]", "", phone_number)

        return new_phone_number

    def __clean_uuid(self, uuid_code):
        """
        Genereates new random uuid if there is not valid.

        Parameters:
            - uuid: str|uuid
        """
        new_uuid = uuid_code
        regex = r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}"
        try:
            if re.search(regex, new_uuid) is None:
                print(new_uuid)
                new_uuid = uuid.uuid4()
        except TypeError:
            new_uuid = uuid.uuid4()

        return new_uuid

    def __clean_integer_number(self, number):
        """
        Convert to integer if the number parameter is string.
        If does not, try to remove any non number digit in that string.
        Parameters:
            number: integer|string
        """
        new_number = number
        try:
            # pd.to_numeric(row["card_number"], errors="coerce")
            new_number = int(new_number)
        except ValueError:
            ValueError("This is not a valid integer number")
            regex_number_card = r"\D+"
            if re.search(regex_number_card, str(number)) is not None:
                new_number = re.sub(r"\D+", "", number)
                if new_number and len(str(new_number)) > 10:
                    new_number = int(new_number)
                else:
                    new_number = None

        return new_number

    def __clean_float_number(self, number):
        """
        Convert to float if the number parameter is string.
        If does not, check if has any non number digit and dot (.) in that string.
        Otherwise, return None.

        Parameters:
            - number: float|string
        """
        new_number = number
        try:
            new_number = float(new_number)
        except (ValueError, TypeError):
            print("This is not a valid float number")
            regex_number_card = r"(?=\D+)(?=\.)"
            if re.search(regex_number_card, str(number)) is None:
                new_number = None

        return new_number

    def __clean_expiry_date(self, expiry_date):
        """
        Parameters:
            - expiry_date: string
        """
        new_expiry_date = expiry_date
        regex_expire_date = r"^(\d{2}/\d{2})$"
        if re.search(regex_expire_date, expiry_date.strip()) is None:
            new_expiry_date = None
        return new_expiry_date

    def __clean_card_provider(self, card_provider):
        """
        Check if the card provider is not in the valid card providers listed names.

        Parameters:
            - card_provider: string
        """
        new_card_provider = card_provider
        if card_provider not in self.__CARD_PROVIDERS:
            new_card_provider = None
        return new_card_provider

    def __clean_store_code(self, store_code):
        """
        Check if the store code has XX-XXXXXXXX pattern, where X is a alphabet or digital number
        Parameters:
            - store_code: str
        """
        new_store_code = store_code
        regex_store_code = r"^([A-Z]{2,3}-[A-Z0-9]{8})$"
        if re.search(regex_store_code, new_store_code) is None:
            print(f"store code: {new_store_code}")
            new_store_code = None
        return new_store_code

    def __clean_store_type(self, store_type):
        """
        Check if the store type is a NULL, N/A string value or is not in the store type list.

        Parameters:
            - store_type: str
        """
        new_store_type = self.__clean_strings(store_type)
        if new_store_type and new_store_type not in self.__STORE_TYPE:
            new_store_type = None
        return new_store_type

    def __convert_weight_from_str_to_float(self, weight, regex):
        """
        Convert string weight to float.

        Parameters:
            - weight: str
            - regex: regular expression -> It could be in KG or G.
        """
        try:
            weight = re.sub(regex, "", weight)
            weight = float(weight)
        except ValueError:
            regex = r"\sx\s"
            if re.search(regex, weight):
                weight_split = weight.split(" x ")
                if len(weight_split) == 2:
                    first_number = int(weight_split[0])
                    second_number = int(weight_split[1])
                    weight = first_number * second_number

        return weight

    def __clean_weight(self, weight):
        """
        Check if the weight measure is in KG.
        If is not, convert the weight to decimal value representing their weight in kg.

        Parameters:
            - weight: str
        """
        new_weight = weight
        regex = r"ml"
        if not isinstance(new_weight, float) and re.search(regex, new_weight):
            new_weight = re.sub(regex, "g", new_weight)

        if isinstance(new_weight, str) and re.search(r"(k|K)(g|G)", new_weight):
            new_weight = self.__convert_weight_from_str_to_float(
                new_weight, r"(k|K|g|G|\s\.)"
            )
        elif isinstance(new_weight, str) and re.search(r"(g|G)", new_weight):
            new_weight = self.__convert_weight_from_str_to_float(
                new_weight, r"(g|G|\s\.)"
            )
            try:
                new_weight /= 1000
            except TypeError as e:
                print(f"Error type with {new_weight}: {e}")
                new_weight = None

        if isinstance(new_weight, str) and re.search(r"(\D)", new_weight):
            new_weight = None

        return new_weight

    def __clean_removed(self, removed):
        if removed not in self.__REMOVED_ITEM:
            return None
        return removed

    def __clean_product_category(self, category):
        if category not in self.__PRODUCTS_CATEGORY:
            return None
        return category

    def __clean_ean(self, ean):
        new_ean = ean
        regex = r"\D"
        try:
            if re.search(regex, ean):
                new_ean = None
        except TypeError:
            new_ean = None
        return new_ean

    def __clean_product_name(self, product_name):
        new_product_name = product_name
        regex = r"[A-Z0-9]{10}"
        try:
            if re.search(regex, product_name):
                new_product_name = None
        except TypeError:
            new_product_name = None
        return new_product_name

    def __clean_product_price(self, product_price):
        new_product_price = product_price
        regex = r"£[\d]{1,}\.[\d]{2}"
        try:
            if re.search(regex, product_price) is None:
                new_product_price = None
        except TypeError:
            new_product_price = None
        return new_product_price

    def __clean_product_code(self, product_code):
        new_product_code = product_code
        regex = r"\w{2}-\w{5,}"
        try:
            if re.search(regex, product_code) is None:
                new_product_code = None
        except TypeError:
            new_product_code = None
        return new_product_code

    def __sort_columns(self, df):
        new_df = df.copy()
        try:
            columns = [column for column in new_df.columns]
            columns.remove("latitude")
            longitude_index = columns.index("longitude")
            columns.insert(longitude_index, "latitude")
            new_df = new_df[columns]
            columns = [column for column in new_df.columns]
            # print(f"reodered columns: {columns}")
        except:
            new_df = df.copy

        return new_df

    def __clean_time_period(self, time_period):
        if time_period not in self.__TIME_PERIOD:
            return None
        return time_period

    def __clean_timestamp(self, timestamp):
        try:
            if datetime.strptime(timestamp, "%H:%M:%S"):
                return timestamp
        except ValueError:
            print(f"timestamp: {timestamp}")
            return None

    def __clean_day(self, day):
        try:
            if int(day) > 0 and int(day) < 32:
                return int(day)
        except ValueError:
            print(f"day: {day}")
            return None

    def __clean_month(self, month):
        try:
            if int(month) > 0 and int(month) < 13:
                return int(month)
        except ValueError:
            print(f"month: {month}")
            return None

    def __clean_year(self, year):
        try:
            if int(year) > 1900 and int(year) < 2100:
                return int(year)
        except ValueError:
            print(f"year: {year}")
            return None

    def clean_user_data(self, table_name):
        data_extractor = DataExtractor()
        df = data_extractor.read_rds_table(DatabaseConnector(), table_name)
        for index, row in df.iterrows():
            df.at[index, "first_name"] = self.__clean_strings(row["first_name"])
            df.at[index, "last_name"] = self.__clean_strings(row["last_name"])
            df.at[index, "company"] = self.__clean_strings(row["company"])
            df.at[index, "address"] = self.__clean_strings(row["address"])
            df.at[index, "date_of_birth"] = self.__clean_date(row["date_of_birth"])
            df.at[index, "join_date"] = self.__clean_date(row["join_date"])
            df.at[index, "email_address"] = self.__clean_email(row["email_address"])
            df.at[index, "country"] = self.__clean_country(
                row["country"], row["country_code"]
            )
            df.at[index, "country_code"] = self.__clean_country_code(
                row["country"], row["country_code"]
            )
            df.at[index, "phone_number"] = self.__clean_phone_number(
                row["phone_number"]
            )
            df.at[index, "user_uuid"] = self.__clean_uuid(row["user_uuid"])

        df = df.convert_dtypes()

        return df

    def clean_card_data(self, df):
        """
        Clean the card data.

        Parameters:
            - df: Dataframe
        """

        for index, row in df.iterrows():
            df.at[index, "card_number"] = self.__clean_integer_number(
                row["card_number"]
            )
            df.at[index, "expiry_date"] = self.__clean_expiry_date(row["expiry_date"])
            # Check if the card provider is not in the valid card providers listed names.
            df.at[index, "card_provider"] = self.__clean_card_provider(
                row["card_provider"]
            )
            # If there is error in the date pattern, try to convert the date in right format (YYYY-mm-dd) or asign None.
            df.at[index, "date_payment_confirmed"] = self.__clean_date(
                row["date_payment_confirmed"]
            )

        df = df.convert_dtypes()
        df["date_payment_confirmed"] = df["date_payment_confirmed"].astype(
            "datetime64[ns]"
        )

        return df

    def clean_store_data(self, df):
        """ """
        df = df.drop(columns=["lat"])
        df = self.__sort_columns(df)

        for index, row in df.iterrows():
            df.at[index, "staff_numbers"] = self.__clean_integer_number(
                row["staff_numbers"]
            )
            df.at[index, "address"] = self.__clean_strings(row["address"])
            df.at[index, "locality"] = self.__clean_strings(row["locality"])
            df.at[index, "store_type"] = self.__clean_store_type(row["store_type"])
            df.at[index, "country_code"] = self.__clean_country_code(
                country_code=row["country_code"]
            )
            df.at[index, "longitude"] = self.__clean_float_number(row["longitude"])
            df.at[index, "latitude"] = self.__clean_float_number(row["latitude"])
            df.at[index, "store_code"] = self.__clean_store_code(row["store_code"])
            df.at[index, "opening_date"] = self.__clean_date(row["opening_date"])
            df.at[index, "continent"] = self.__clean_continet(row["continent"])

        return df

    def convert_product_weights(self, df):
        for index, row in df.iterrows():
            df.at[index, "weight"] = self.__clean_weight(row["weight"])

        return df

    def clean_products_data(self, df):
        df.rename(columns={"EAN": "ean"}, inplace=True)
        for index, row in df.iterrows():
            df.at[index, "product_name"] = self.__clean_product_name(
                row["product_name"]
            )
            df.at[index, "product_price"] = self.__clean_product_price(
                row["product_price"]
            )
            df.at[index, "date_added"] = self.__clean_date(row["date_added"])
            df.at[index, "uuid"] = self.__clean_uuid(row["uuid"])
            df.at[index, "removed"] = self.__clean_removed(row["removed"])
            df.at[index, "category"] = self.__clean_product_category(row["category"])
            df.at[index, "ean"] = self.__clean_ean(row["ean"])
            df.at[index, "product_code"] = self.__clean_product_code(
                row["product_code"]
            )

        return df

    def clean_orders_data(self, df):
        df = df.drop(["level_0", "first_name", "last_name", "1"], axis=1)
        for index, row in df.iterrows():
            df.at[index, "date_uuid"] = self.__clean_uuid(row["date_uuid"])
            df.at[index, "user_uuid"] = self.__clean_uuid(row["user_uuid"])
            df.at[index, "card_number"] = self.__clean_integer_number(
                row["card_number"]
            )
            df.at[index, "store_code"] = self.__clean_store_code(row["store_code"])
            df.at[index, "store_code"] = self.__clean_store_code(row["store_code"])
            df.at[index, "product_code"] = self.__clean_product_code(
                row["product_code"]
            )
            df.at[index, "product_quantity"] = self.__clean_integer_number(
                row["product_quantity"]
            )

        return df

    def clean_date_details(self, df):
        for index, row in df.iterrows():
            df.at[index, "date_uuid"] = self.__clean_uuid(row["date_uuid"])
            df.at[index, "time_period"] = self.__clean_time_period(row["time_period"])
            df.at[index, "timestamp"] = self.__clean_timestamp(row["timestamp"])
            df.at[index, "day"] = self.__clean_day(row["day"])
            df.at[index, "month"] = self.__clean_month(row["month"])
            df.at[index, "year"] = self.__clean_year(row["year"])

        return df


if __name__ == "__main__":
    data_cleaning = DataCleaning()
    data_extractor = DataExtractor()
    db = DatabaseConnector()

    # TASK 8
    url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    df = data_extractor.extract_json_from_s3(url)
    df = data_cleaning.clean_date_details(df)
    columns_type = {"index": BigInteger}
    db.upload_to_db(df, "dim_date_times", column_types=columns_type)

    # TASK 7
    # tables = data_extractor.list_db_tables()
    # df = data_extractor.read_rds_table(DatabaseConnector(), tables[-1])
    # df = data_cleaning.clean_orders_data(df)
    # db.upload_to_db(df, "orders_table")

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
    # df = data_extractor.retrieve_stores_data(100)
    # df_cleaned = data_cleaning.clean_store_data(df)
    # columns_type = {"latitude": Float, "longitude": Float}
    # db.upload_to_db(df_cleaned, "dim_store_details", columns_type)

    # TASK 4
    # pdf_path = (
    #     "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    # )
    # df = data_extractor.retrieve_pdf_data(pdf_path)
    # df = data_cleaning.clean_card_data(df)
    # columns_types = {"card_number": BigInteger, "date_payment_confirmed": Date}
    # db.upload_to_db(df, "dim_card_details", columns_types)

    # TASK 3
    # df = data_cleaning.clean_user_data("legacy_users")
    # columns_types = {"date_of_birth": Date, "join_date": Date, "user_uuid": UUID}
    # db.upload_to_db(df, "dim_users", columns_types)
