from datetime import datetime
import re
from typing import Union, List
import uuid

import pandas as pd
from pandas import DataFrame


class BaseDataCleaner:

    _INDEX = "index"
    _INDEX_ADDRESS = "address"
    _INDEX_CARD_NUMBER = "card_number"
    _INDEX_CARD_PROVIDER = "card_provider"
    _INDEX_COMPANY = "company"
    _INDEX_CATEGORY = "category"
    _INDEX_CONTINENT = "continent"
    _INDEX_COUNTRY = "country"
    _INDEX_COUNTRY_CODE = "country_code"
    _INDEX_DATE_ADDED = "date_added"
    _INDEX_DATE_OF_BIRTH = "date_of_birth"
    _INDEX_DATE_PAYMENT_CONFIRMED = "date_payment_confirmed"
    _INDEX_DATE_UUID = "date_uuid"
    _INDEX_DAY = "day"
    _INDEX_EMAIL_ADDRESS = "email_address"
    _INDEX_EAN = "ean"
    _INDEX_EXPIRY_DATE = "expiry_date"
    _INDEX_FIRST_NAME = "first_name"
    _INDEX_JOIN_DATE = "join_date"
    _INDEX_LAST_NAME = "last_name"
    _INDEX_LAT = "lat"
    _INDEX_LATITUDE = "latitude"
    _INDEX_LOCALITY = "locality"
    _INDEX_LONGITUDE = "longitude"
    _INDEX_MONTH = "month"
    _INDEX_OPENING_DATE = "opening_date"
    _INDEX_PHONE_NUMBER = "phone_number"
    _INDEX_PRODUCT_CODE = "product_code"
    _INDEX_PRODUCT_NAME = "product_name"
    _INDEX_PRODUCT_QUANTITY = "product_quantity"
    _INDEX_PRODUCT_PRICE = "product_price"
    _INDEX_REMOVED = "removed"
    _INDEX_STAFF_NUMBERS = "staff_numbers"
    _INDEX_STORE_TYPE = "store_type"
    _INDEX_STORE_CODE = "store_code"
    _INDEX_TIME_PERIOD = "time_period"
    _INDEX_TIMESTAMP = "timestamp"
    _INDEX_USER_UUID = "user_uuid"
    _INDEX_UUID = "uuid"
    _INDEX_WEIGHT = "weight"
    _INDEX_YEAR = "year"

    _STRING_NULL = "NULL"
    _STRING_NA = "N/A"

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

    def clean_strings(self, string: str) -> Union[str, None]:
        """
        * Parameters:
            - string: string
        """
        if (
            not string
            or string == self._STRING_NULL
            or string == self._STRING_NA
            or re.search(r"\d", string)
        ):
            string = None
        return string

    def clean_strings_with_numbers(self, string: str) -> Union[str, None]:
        """ """
        if not string or string == self._STRING_NULL or string == self._STRING_NA:
            string = None
        return string

    def __check_date_format(self, date: str, format: str) -> bool:
        """
        Check if the date string is in date format.

        * Parameters:
            - date: string
            - format: string -> Pattern of date
        """
        is_format: bool = True
        try:
            is_format = bool(datetime.strptime(date, format))
        except ValueError:
            is_format = False
        except TypeError:
            is_format = False

        return is_format

    def clean_date(self, date: str) -> datetime:
        """
        Convert datetime fields to %Y-%m-%d format. Otherwise,  or None.

        * Parameters:
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

    def clean_continent(self, continent: str) -> Union[str, None]:
        """
        Check if the continent has the right classification. Otherwise, try to clean it.

        * Parameters:
            - email: string
        """
        new_continent: str = continent
        if new_continent not in self.__CONTINENTS:
            regex = r"((\w)\2{1,})"
            if re.search(regex, new_continent):
                new_continent = re.sub(regex, "", new_continent)
            else:
                new_continent = None

        return new_continent

    def clean_email(self, email: str) -> Union[str, None]:
        """
        Clean the email that contains 0, 2 or more '@' character(s).

        * Parameters:
            - email: string
        """
        regex: str = r"^[\w0-9_.+-]+@[\w-]{2,}\.[\w]{2,}[\.\w]{0,}$"
        new_email: str = email
        if re.search(regex, email) is None:
            at_regex = r"@{2,}"
            if re.search(at_regex, email):
                new_email = re.sub(at_regex, "@", email)
            elif re.search(r"[^@]", email):
                new_email = None

        return new_email

    def clean_country(self, country: str, country_code: str) -> Union[str, None]:
        """
        * Parameters:
            - country: string
            - country_code: string
        """
        new_country: str = country
        for key, value in self.__MAP_COUNTRY_CODE.items():
            if key == country_code and country not in self.__COUNTRIES:
                new_country = value
        if country == self._STRING_NULL or re.search(r"\d", country):
            new_country = None

        return new_country

    def clean_country_code(
        self, country: str = None, country_code: str = None
    ) -> Union[str, None]:
        """
        * Parameters:
            - country: string
            - country_code: string
        """
        new_country_code: str = country_code
        if country is None:
            try:
                if self.__MAP_COUNTRY_CODE[new_country_code]:
                    return new_country_code
            except KeyError as e:
                print(e)
                for key, value in self.__MAP_COUNTRY_CODE.items():
                    if value == country:
                        new_country_code = key
                new_country_code = None

        else:
            country_codes = [key for key, value in self.__MAP_COUNTRY_CODE.items()]
            if new_country_code not in country_codes:
                new_country_code = None

        return new_country_code

    def clean_phone_number(self, phone_number: str) -> str:
        """
        Removes any alphabetic and special character (except plus sign) from the phone numbers.

        * Parameters:
            - phone_number: string
        """
        new_phone_number = phone_number
        regex = r"[a-zA-Z-\s\-\.\(\)]+"
        if phone_number == self._STRING_NULL:
            new_phone_number = None
        elif re.search(regex, phone_number) is not None:
            new_phone_number = re.sub(r"[a-zA-Z\(\)\s\-\.]", "", phone_number)

        return new_phone_number

    def clean_uuid(self, uuid_code: str) -> Union[str]:
        """
        Genereates new random uuid if uuid code is not valid.

        * Parameters:
            - uuid: str|uuid
        """
        new_uuid: str = uuid_code
        regex = r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}"
        try:
            if re.search(regex, new_uuid) is None:
                new_uuid = uuid.uuid4()
        except TypeError:
            new_uuid = uuid.uuid4()

        return new_uuid

    def clean_integer_number(
        self, number: Union[str, int], min_digits: int = 10
    ) -> Union[int, None]:
        """
        Convert to integer if the number parameter is string.
        If does not, try to remove any non number digit in that string.

        * Parameters:
            - number: integer|string
        """
        new_number = number
        try:
            # pd.to_numeric(row["card_number"], errors="coerce")
            new_number = int(new_number)
        except ValueError:
            ValueError("This is not a valid integer number")
            regex_number = r"\D+"
            if re.search(regex_number, str(number)) is not None:
                new_number = re.sub(r"\D+", "", number)
                if new_number and len(str(new_number)) > min_digits:
                    new_number = int(new_number)
                else:
                    new_number = None

        return new_number

    def clean_float_number(self, number: Union[str, float]) -> Union[float, None]:
        """
        Convert to float if the number parameter is string.
        If does not, check if has any non number digit and dot (.) in that string.
        Otherwise, return None.

        * Parameters:
            - number: string|float
        """
        new_number: Union[str, float] = number
        try:
            new_number = float(new_number)
        except (ValueError, TypeError):
            regex_number = r"(?=\D+)(?=\.)"
            if re.search(regex_number, str(number)) is None:
                new_number = None

        return new_number

    def clean_expiry_date(self, expiry_date: str) -> Union[str, None]:
        """
        * Parameters:
            - expiry_date: string
        """
        new_expiry_date: str = expiry_date
        regex_expire_date = r"^(\d{2}/\d{2})$"
        if re.search(regex_expire_date, expiry_date.strip()) is None:
            new_expiry_date = None
        return new_expiry_date

    def clean_card_provider(self, card_provider: str) -> Union[str, None]:
        """
        Check if the card provider is not in the valid card providers listed names.

        * Parameters:
            - card_provider: string
        """
        new_card_provider: str = card_provider
        if card_provider not in self.__CARD_PROVIDERS:
            new_card_provider = None
        return new_card_provider

    def clean_store_code(self, store_code: str) -> Union[str, None]:
        """
        Check if the store code has XX-XXXXXXXX pattern, where X is a alphabet or digital number.

        * Parameters:
            - store_code: string
        """
        new_store_code: str = store_code
        regex_store_code = r"^([A-Z]{2,3}-[A-Z0-9]{8})$"
        if re.search(regex_store_code, new_store_code) is None:
            new_store_code = None
        return new_store_code

    def clean_store_type(self, store_type: str) -> Union[str, None]:
        """
        Check if the store type is a NULL, N/A string value or is not in the store type list.

        * Parameters:
            - store_type: string
        """
        new_store_type: str = self.clean_strings_with_numbers(store_type)
        if new_store_type and new_store_type not in self.__STORE_TYPE:
            new_store_type = None
        return new_store_type

    def clean_weight(self, weight: str) -> Union[float, None]:
        """
        Check if the weight measure is in KG.
        If is not, convert the weight to decimal value representing their weight in kg.

        * Parameters:
            - weight: string
        """
        new_weight: str = weight
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
                print(f"Error type with the weight {new_weight}: {e}")
                new_weight = None

        if isinstance(new_weight, str) and re.search(r"(\D)", new_weight):
            new_weight = None

        return new_weight

    def clean_removed(self, removed: str) -> Union[str, None]:
        """
        Verify if the removed has the right classification.

        * Parameters:
            - removed: string
        """
        if removed not in self.__REMOVED_ITEM:
            return None
        return removed

    def clean_product_category(self, category: str) -> Union[str, None]:
        """
        Verify if the category has the right classification.

        * Parameters:
            - category: string
        """
        if category not in self.__PRODUCTS_CATEGORY:
            return None
        return category

    def clean_ean(self, ean: str) -> Union[str, None]:
        """
        Verify if the ean is a valid number.

        * Parameters:
            - ean: string|integer
        """
        new_ean: str = ean
        regex = r"\D"
        try:
            if re.search(regex, ean):
                new_ean = None
        except TypeError:
            new_ean = None
        return new_ean

    def clean_product_name(self, product_name: str) -> Union[str, None]:
        """
        Verify if the product name is a valid string.

        * Parameters:
            - product_name: string
        """
        new_product_name: str = product_name
        regex = r"[A-Z0-9]{10}"
        try:
            if re.search(regex, product_name):
                new_product_name = None
        except TypeError:
            new_product_name = None
        return new_product_name

    def clean_product_price(self, product_price: str) -> Union[str, None]:
        """
        Verify if the product price is in the format £x.xx.

        where:
            x : string belongs to integer number.

        * Parameters:
            - product_price: string
        """
        new_product_price: str = product_price
        regex = r"£[\d]{1,}\.[\d]{2}"
        try:
            if re.search(regex, product_price) is None:
                new_product_price = None
        except TypeError:
            new_product_price = None
        return new_product_price

    def clean_product_code(self, product_code: str) -> Union[str, None]:
        """
        Verify if the product code is in the format xx-yyyyy.

        where:
            x : string belongs to alphabet.
            y : string belongs to alphabet or number.

        And product code could has more than 5 characters.

        * Parameters:
            - product_code: string
        """
        new_product_code: str = product_code
        regex = r"\w{2}-\w{5,}"
        try:
            if re.search(regex, product_code) is None:
                new_product_code = None
        except TypeError:
            new_product_code = None
        return new_product_code

    def clean_time_period(self, time_period: str) -> Union[str, None]:
        """
        Verify if time period has the right classification.

        * Parameters:
            - time_period: string
        """
        if time_period not in self.__TIME_PERIOD:
            return None
        return time_period

    def clean_timestamp(self, timestamp: str) -> Union[str, None]:
        """
        Verify if the timestamp is in format HH:MM:SS.

        * Parameters:
            - timestamp: string
        """
        try:
            if datetime.strptime(timestamp, "%H:%M:%S"):
                return timestamp
        except ValueError:
            return None

    def clean_day(self, day: Union[int, str]) -> Union[int, None]:
        """
        Verify if the day is a valid day number.

        * Parameters:
            - day: int|string
        """
        try:
            if int(day) > 0 and int(day) < 32:
                return int(day)
        except ValueError:
            return None

    def clean_month(self, month: Union[int, str]) -> Union[int, None]:
        """
        Verify if the month is a valid month number.

        * Parameters:
            - month: int|string
        """
        try:
            if int(month) > 0 and int(month) < 13:
                return int(month)
        except ValueError:
            return None

    def clean_year(self, year: Union[int, str]) -> Union[int, None]:
        """
        Verify if the year is a valid year number.

        * Parameters:
            - year: int|string
        """
        try:
            if int(year) > 1900 and int(year) < 2100:
                return int(year)
        except ValueError:
            return None

    def __convert_weight_from_str_to_float(self, weight: str, regex: str) -> float:
        """
        Convert string weight to float.

        * Parameters:
            - weight: string
            - regex: regular expression -> It could be in KG or G.
        """
        try:
            weight: str = re.sub(regex, "", weight)
            weight: float = float(weight)
        except ValueError:
            regex = r"\sx\s"
            if re.search(regex, weight):
                weight_split: List[str] = weight.split(" x ")
                if len(weight_split) == 2:
                    first_number = int(weight_split[0])
                    second_number = int(weight_split[1])
                    weight = first_number * second_number

        return weight

    def sort_store_columns(self, df: DataFrame) -> DataFrame:
        """
        Sort columns of store data.

        * Parameters:
            - df: Dataframe
        """
        new_df: DataFrame = df.copy()
        try:
            columns: List[str] = [column for column in new_df.columns]
            columns.remove(self._INDEX_LATITUDE)
            longitude_index: int = columns.index(self._INDEX_LONGITUDE)
            columns.insert(longitude_index, self._INDEX_LATITUDE)
            new_df: DataFrame = new_df[columns]
            columns: List[str] = [column for column in new_df.columns]
        except:
            new_df: DataFrame = df.copy()

        return new_df
