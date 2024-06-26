import pandas as pd
import tabula
import requests
import yaml

from sqlalchemy import text
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from database_utils import DatabaseConnector


class DataExtractor:

    def __init__(self):
        self.config = self.read_config()

    def read_config(self):
        """
        Read config file
        """
        with open("config/config.yaml", "r") as file:
            config = yaml.safe_load(file)
        return config

    def __http_get_request(self, url):
        header = {"x-api-key": self.config["x-api-key"]}
        response = requests.get(url, headers=header)
        # print(f"url: {url}")
        # print(f"STATUS: {response.status_code}")
        if response.status_code != 200:
            return None

        return response.json()

    def list_db_tables(self):
        """
        Get all tables from postgres database where schema is 'public'
        """
        db = DatabaseConnector()
        engine = db.init_db_engine()
        connection = engine.connect()
        tables = connection.execute(
            text(
                "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
            )
        )
        return [table[1] for table in tables]

    def read_rds_table(self, db, table_name):
        """
        Read data from postgres table using pandas.
        """
        engine = db.init_db_engine()
        sql = f"SELECT * FROM {table_name}"
        df = pd.read_sql(sql, con=engine)
        return df

    def retrieve_pdf_data(self, pdf_path):
        """
        Extract data from pdf documents.
        """
        dfs = tabula.read_pdf(pdf_path, pages="all")
        df = pd.concat(dfs).reset_index(drop=True)
        return df

    def list_number_of_stores(self):
        """
        Return total number of stores using REST API.
        """
        url = self.config["aws_url_number_of_store"]
        data = self.__http_get_request(url)
        return data["number_stores"]

    def retrieve_stores_data(self, number_of_stores):
        """
        Retrieve each store data via rest api.
        Parameters:
            - number_of_stores: int -> total number of stores
        """
        list_stores_data = []
        for store_number in range(number_of_stores):
            # url = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
            url = f"{self.config['aws_url_store_details']}/{store_number}"
            data = self.__http_get_request(url)
            if data:
                list_stores_data.append(data)

        df = pd.DataFrame.from_records(list_stores_data)
        # [print(column) for column in df.columns]
        return df

    def extract_from_s3(self, bucket_name, bucket_filename, destiny_filename):
        """
        Download and extract the information from the file stored in S3 AWS service.
        """
        try:
            s3 = boto3.client("s3")
            s3.download_file(
                bucket_name,
                bucket_filename,
                destiny_filename,
            )
            df = pd.read_csv(destiny_filename, index_col=0)
            return df
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucket":
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)
            return None
        except FileNotFoundError as e:
            print(e)
            return None

    def extract_json_from_s3(self, url):

        df = None
        json_data = self.__http_get_request(url)
        if json_data:
            df = pd.DataFrame.from_records(json_data)
        # [print(column) for column in df.columns]
        return df


if __name__ == "__main__":
    data_extractor = DataExtractor()
    url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    json_data = data_extractor.extract_json_from_s3(url)
    # print(json_data)

    # tables = data_extractor.list_db_tables()
    # df = data_extractor.read_rds_table(DatabaseConnector(), tables[-1])
    # print(tables[-1])

    # data_extractor.extract_from_s3(
    #     "data-handling-public", "products.csv", "data/products.csv"
    # )

    # number_of_stores = data_extractor.list_number_of_stores()
    # print(f"number of stores: {number_of_stores}")
    # df = data_extractor.retrieve_stores_data(number_of_stores)
    # print(df.at[2, "address"])

    # pdf_path = (
    #     "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    # )
    # df = data_extractor.retrieve_pdf_data(pdf_path)
    # print(df)

    # tables = data_extractor.list_db_tables()
    # print(tables)
    # df = data_extractor.read_rds_table(DatabaseConnector(), tables[1])
    # print(df.dtypes)
    # for index, row in df.iterrows():
    #     print(row)
