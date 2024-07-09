from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

from sqlalchemy import (
    BigInteger,
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


# TASK 3
class Tasks(DataCleaning, DatabaseConnector, DataExtractor):

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

    def upload_dim_(self):
        """
        This function will execute the tasks implemented below:
            TASK 3 in MILESTONE 2
            TASK 2 in MILESTONE 3
            TASK 8 in MILESTONE 3
            TASK 9 in MILESTONE 3
        """

    # def upload_dim_(self):
    #     """
    #     This function will execute the tasks implemented below:
    #         TASK 3 in MILESTONE 2
    #         TASK 2 in MILESTONE 3
    #         TASK 8 in MILESTONE 3
    #         TASK 9 in MILESTONE 3
    #     """


if __name__ == "__main__":
    tasks = Tasks()
    # tasks.upload_dim_users()
    tasks.upload_dim_card_details()
