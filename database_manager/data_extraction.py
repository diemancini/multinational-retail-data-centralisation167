import pandas as pd
import tabula
import requests
import yaml
from typing import Dict, List, Union

from pandas import DataFrame
from sqlalchemy import Connection, CursorResult, Engine, text
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from .database_utils import DatabaseConnector


class DataExtractor:

    def __init__(self):
        self.config = self.read_config()

    def read_config(self) -> Dict:
        """
        Read config file
        """
        with open("config/config.yaml", "r") as file:
            config = yaml.safe_load(file)
        return config

    def __http_get_request(self, url: str) -> Union[Dict, None]:
        """
        * Parameters:
            - url: string
        """
        header: dict = {"x-api-key": self.config["x-api-key"]}
        response = requests.get(url, headers=header)
        if response.status_code != 200:
            return None

        return response.json()

    def list_db_tables(self) -> List[str]:
        """
        Get all tables from postgres database where schema is 'public'
        """
        db: DatabaseConnector = DatabaseConnector()
        engine: Engine = db.init_db_engine(is_localhost=False)
        connection: Connection = engine.connect()
        tables: CursorResult = connection.execute(
            text(
                "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
            )
        )
        return [table[1] for table in tables]

    def read_rds_table(self, db: DatabaseConnector, table_name: str) -> DataFrame:
        """
        Read data from postgres table using pandas.
        Parameters:
            - db: DatabaseConnector
            - table_name: string
        """
        engine: Engine = db.init_db_engine(is_localhost=False)
        sql: str = f"SELECT * FROM {table_name}"
        df: DataFrame = pd.read_sql(sql, con=engine)
        return df

    def retrieve_pdf_data(self, pdf_path: str) -> DataFrame:
        """
        Extract data from pdf documents.

        * Parameters:
            - pdf_path: string
        """
        dfs: DataFrame = tabula.read_pdf(pdf_path, pages="all")
        df: DataFrame = pd.concat(dfs).reset_index(level=0)
        return df

    def list_number_of_stores(self) -> str:
        """
        Return total number of stores using REST API.
        """
        url: str = self.config["aws_url_number_of_store"]
        data: Union[Dict, None] = self.__http_get_request(url)
        return data["number_stores"]

    def retrieve_stores_data(self, number_of_stores: int) -> DataFrame:
        """
        Retrieve each store data via rest api.
        * Parameters:
            - number_of_stores: int -> total number of stores
        """
        list_stores_data: List[Dict] = []
        for store_number in range(number_of_stores):
            url: str = f"{self.config['aws_url_store_details']}/{store_number}"
            data: Dict = self.__http_get_request(url)
            if data:
                list_stores_data.append(data)

        df: DataFrame = pd.DataFrame.from_records(list_stores_data)
        return df

    def extract_from_s3(
        self, bucket_name: str, bucket_filename: str, destiny_filename: str
    ) -> Union[DataFrame, None]:
        """
        Download and extract the information from the file stored in S3 AWS service.

        * Parameters:
            - bucket_name: string
            - bucket_filename: string
            - destiny_filename: string
        """
        try:
            s3 = boto3.client("s3")
            s3.download_file(
                bucket_name,
                bucket_filename,
                destiny_filename,
            )
            df: DataFrame = pd.read_csv(destiny_filename, index_col=0)
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

    def extract_json_from_s3(self, url: str) -> Union[DataFrame, None]:
        """
        * Parameters:
            - url: string
        """
        df = None
        json_data = self.__http_get_request(url)
        if json_data:
            df = pd.DataFrame.from_records(json_data)
        return df
