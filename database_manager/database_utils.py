from typing import Dict, List, Union

from .base_database_connector import BaseDatabaseConnector

import pandas as pd
from pandas import DataFrame

from sqlalchemy import Engine
from sqlalchemy import exc


class DatabaseConnector(BaseDatabaseConnector):

    def read_db_creds(self, filename: str) -> Dict:
        """
        Read credentials DB.

        * Parameters:
            - filename: string
        """
        return self._read_db_creds(filename)

    def init_db_engine(self, is_localhost: bool = True) -> Engine:
        """
        Create a engine for postgress DataBase.

        * Parameters:
            - is_localhost: boolean
        """
        return self._create_engine(is_localhost=is_localhost)

    def upload_to_db(
        self,
        df: DataFrame,
        table_name: str,
        column_types: Dict = None,
        using_query: bool = False,
    ):
        """
        Upload the Dataframe data to localhost database.

        * Parameters:
            - df: DataFrame,
            - table_name: string,
            - column_types: Dict,
            - using_query: boolean -> If this option is True, the table will be created and filled with manual sql queries.
        """
        engine: Engine = self._create_engine()
        self._drop_table(engine, table_name)
        if using_query:
            self._create_table(engine, table_name, column_types)
            self._insert_data(engine, table_name, df)
        else:
            try:
                df.to_sql(
                    table_name,
                    con=engine,
                    if_exists="replace",
                    dtype=column_types,
                    index=False,
                )
            except exc.InternalError as e:
                print(e)

    def add_primary_key_on_column(self, table_name: str, column_name: str):
        """
        * Parameters:
            - table_name: string
            - column_name: string
        """
        engine: Engine = self._create_engine()

        alter_query: str = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name})"
        self.commit_db(engine=engine, query=alter_query)

    def add_foreign_key(
        self, child_table: str, parent_table: str, fk_column: str, pk_column
    ):
        """ """
        engine: Engine = self._create_engine()
        alter_query = f"ALTER TABLE {child_table}\n\t"
        alter_query += f"ADD CONSTRAINT {parent_table}_{child_table}_fk\n\t"
        alter_query += (
            f"FOREIGN KEY ({fk_column}) REFERENCES {parent_table} ({pk_column});"
        )
        self.commit_db(engine=engine, query=alter_query)

    def update_datatypes_db(
        self,
        table_name: str,
        column_types: List[Union[str, int]] = None,
    ):
        """
        Upload the Dataframe data to localhost databascrede.

        * Parameters:
            - df: DataFrame
            - table_name: string
            - column_types: Dict
            - is_constraints: boolean
        """
        engine: Engine = self._create_engine()
        update_query: str = f"ALTER TABLE {table_name}\n"
        for column in column_types:
            datatype: str = self._convert_sqlalquemy_data_type_to_string(
                column[self._COLUMN_TYPE]
            )
            update_query += f"ALTER COLUMN {column[self._COLUMN_NAME]} TYPE {datatype} USING ({column[self._COLUMN_NAME]}::{datatype})"
            if len(column) == 3:
                update_query += f"{column[2]}"
            update_query += ",\n"

        update_query = update_query[:-2] + ";"
        self.commit_db(engine=engine, query=update_query)

    def create_new_column(self, table_name: str, columns: List[Union[str]]):
        """
        * Parameters:
            - table_name: string
            - columns: List
        """
        engine: Engine = self._create_engine()
        for column in columns:
            datatype: str = self._convert_sqlalquemy_data_type_to_string(
                column[self._COLUMN_TYPE]
            )
            new_column_query: str = f"ALTER TABLE {table_name}\n"
            new_column_query += (
                f"ADD IF NOT EXISTS {column[self._COLUMN_NAME]} {datatype}"
            )
            self.commit_db(engine=engine, query=new_column_query)

    def select_db(self, select_query: str):
        """
        * Parameters:
            - select_query: string
        """
        engine: Engine = self.init_db_engine()
        pd.set_option("display.max_colwidth", None)
        # pd.set_option("display.html.table_schema", True)
        df: DataFrame = pd.read_sql(select_query, con=engine)
        return df

    def insert_db(self, insert_query: str, output: bool = True):
        """
        * Parameters:
            - insert_query: string
        """
        engine: Engine = self.init_db_engine()
        self.commit_db(engine=engine, query=insert_query, output=output)

    def rename_column_name(
        self, table_name: str, column_name: str, new_column_name: str
    ):
        """
        * Parameters:
            - table_name: string
            - old_name: string
            - new_name: string
        """
        engine: Engine = self.init_db_engine()
        alter_query = f"ALTER TABLE IF EXISTS {table_name} RENAME COLUMN {column_name} TO {new_column_name};"
        self.commit_db(engine=engine, query=alter_query)
