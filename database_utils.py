import re
from typing import Dict, List, Union
from psycopg2 import ProgrammingError
import yaml

import pandas as pd
from pandas import DataFrame


from sqlalchemy import Engine, create_engine, text
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Float,
    Integer,
    MetaData,
    NVARCHAR,
    SmallInteger,
    Table,
    UUID,
    VARCHAR,
    exc,
)

# from sqlalchemy import UUID


class DatabaseConnector:

    def __convert_sqlalquemy_data_type_to_string(self, column: str) -> str:
        if column.__visit_name__ == "small_integer":
            return "SMALLINT"
        elif column.__visit_name__ == "VARCHAR":
            length = column.__getattribute__("length")
            return f"{column.__visit_name__}({length})"
        # elif column.__visit_name__ == "":
        else:
            return column.__visit_name__

    def __create_table(self, engine, table_name: str, columns: List[str]):
        """
        Create dim table in repository with hardcode query.
        The df.to_sql creates the table and insert data automatically, making this function unnecessary.

        Parameters:
            - engine:
            - table_name: string
            - columns: string
        """
        # print(columns)
        # columns_metadata = []
        # for column in columns:
        #     print(item for item in column)
        #     columns_metadata.append(Column(column[0], column[1]))

        # metadata = MetaData()
        # print(columns_metadata)
        # # Define a table with a Float column
        # example_table = Table(
        #     f"{table_name}",
        #     metadata,
        #     ((columns_metadata[i],) for i in range(len(columns_metadata))),
        #     # columns_metadata,
        #     # Column(f"{column_name}", Integer, primary_key=True),
        #     # Column('value', Float)
        # )

        # # Create the table
        # metadata.create_all(engine, tables=[example_table])

        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        for column in columns:
            column[1] = self.__convert_sqlalquemy_data_type_to_string(column[1])
            for attribute in column:
                create_query += f"\t\t{ attribute}"
            create_query += f",\n"
            # if isinstance(column[1], tuple):
            # create_query += f"{column_type[0]} {column_type[1]},\n"
            # else:
            # create_query += f"{column_type[1].adapt()},\n"

        # if column_[0]"index":
        #     create_query += f"\t\t{column_name} integer NOT NULL PRIMARY KEY,\n"
        # elif column_name == "user_uuid":
        #     create_query += (
        #         f"\t\t{column_name} UUID DEFAULT uuid_generate_v4(),\n"
        #         # f"\t\t{column_name} UUID NOT NULL DEFAULT uuid_generate_v4(),\n"
        #     )
        # elif type == 'string':
        #     create_query += f"\t\t{column_name} varchar(40),\n"
        # elif type == "datetime64[ns]":
        #     create_query += f"\t\t{column_name} date,\n"
        create_query = create_query[:-2] + "\n);"
        print(create_query)
        self.commit_db(engine=engine, query=create_query)
        # try:
        #     uuid_extension = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
        #     with engine.connect() as connection:
        #         connection.execute(text(uuid_extension))
        #         connection.execute(text(create_query))
        #         connection.commit()
        # except exc.ProgrammingError as e:
        #     print(e)

    def __drop_table(self, engine: Engine, table_name: str):
        """
        Parameters:
            - engine: Engine
            - table_name: str)
        """
        delete_query = f"DROP TABLE {table_name};"
        self.commit_db(engine=engine, query=delete_query)

    def __insert_data(self, engine, table_name, df):
        # print(df.columns.to_list())
        columns = tuple(df.columns.to_list())
        insert_query = f"INSERT INTO {table_name} "
        insert_query += f"{columns} VALUES\n("
        # insert_query += f"(index, address, latitude, longitude, locality, store_code, staff_numbers, opening_date, store_type, country_code, continent) VALUES\n"
        for column in columns:
            insert_query += f":{column}, "
        insert_query = insert_query[:-2] + ");"
        # insert_query += f"(:index, :address, :latitude, :longitude, :locality, :store_code, :staff_numbers, :opening_date, :store_type, :country_code, :continent)"
        insert_query = re.sub(r"'", "", insert_query)
        print(insert_query)
        data = df.to_dict(orient="records")
        self.commit_db(engine=engine, query=insert_query, data=data)
        # uuid_extension = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
        # try:
        #     with connection.connect() as connection:
        #         connection.execute(text(uuid_extension))
        #         connection.execute(text(insert_query), data)
        #         connection.commit()
        # except ProgrammingError as e:
        #     print(e)

    def commit_db(self, engine: Engine, query: str, data: List = None):
        """
        Parameters:
            - engine: Engine
            - query: string
        """
        try:
            uuid_extension = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
            with engine.connect() as connection:
                connection.execute(text(uuid_extension))
                connection.execute(text(query), data)
                connection.commit()
        except exc.ProgrammingError as e:
            print(e)
        except exc.IntegrityError as e:
            print(e)

    def read_db_creds(self, filename: str) -> Dict:
        """
        Read credentials DB
        Parameters:
            - filename: str
        """
        with open(filename, "r") as file:
            creds: Dict = yaml.safe_load(file)
        return creds

    def init_db_engine(self) -> Engine:
        """
        Create a engine for postgress DataBase
        """
        creds: Dict = self.read_db_creds("config/db_creds.yaml")
        # Database URLs sintax: dialect+driver://username:password@host:port/database
        return create_engine(
            f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )

    def init_local_db_engine(self):
        creds: Dict = self.read_db_creds("config/db_creds_local.yaml")
        engine: Engine = create_engine(
            f"postgresql+psycopg2://{creds['LOCALHOST_USER']}:{creds['LOCALHOST_PASSWORD']}@{creds['LOCALHOST_HOST']}:{creds['LOCALHOST_PORT']}/{creds['LOCALHOST_DATABASE']}"
        )
        return engine

    def upload_to_db(
        self,
        df: DataFrame,
        table_name: str,
        column_types: Dict = None,
        is_constraints: bool = False,
        drop_table: bool = False,
    ):
        """
        Upload the Dataframe data to localhost databascrede.

        Parameters:
            df: DataFrame,
            table_name: str,
            column_types: Dict,
            is_constraints: boolean,
            drop_table: boolean,
        """
        creds: Dict = self.read_db_creds("config/db_creds_local.yaml")
        engine: Engine = create_engine(
            f"postgresql+psycopg2://{creds['LOCALHOST_USER']}:{creds['LOCALHOST_PASSWORD']}@{creds['LOCALHOST_HOST']}:{creds['LOCALHOST_PORT']}/{creds['LOCALHOST_DATABASE']}"
        )
        # type_of_insert: str = "replace"
        if drop_table:
            self.__drop_table(engine, table_name)
        if is_constraints:
            self.__create_table(engine, table_name, column_types)
            self.__insert_data(engine, table_name, df)
        else:
            # type_of_insert = "append"
            # column_types = None

            # print(df["address"])
            #
            df.to_sql(
                table_name,
                con=engine,
                if_exists="replace",
                dtype=column_types,
                index=False,
            )

    def add_primary_key_on_column(self, table_name: str, column_name: str):
        """
        Parameters:
            - table_name: string
            - column_name: string
        """
        creds: Dict = self.read_db_creds("config/db_creds_local.yaml")
        engine: Engine = create_engine(
            f"postgresql+psycopg2://{creds['LOCALHOST_USER']}:{creds['LOCALHOST_PASSWORD']}@{creds['LOCALHOST_HOST']}:{creds['LOCALHOST_PORT']}/{creds['LOCALHOST_DATABASE']}"
        )

        alter_query: str = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name})"
        print(alter_query)
        self.commit_db(engine=engine, query=alter_query)

    def add_foreign_key(
        self, child_table: str, parent_table: str, fk_column: str, pk_column
    ):
        """ """
        # ALTER TABLE child_table
        # ADD CONSTRAINT constraint_name
        # FOREIGN KEY (fk_columns)
        # REFERENCES parent_table (parent_key_columns);
        creds: Dict = self.read_db_creds("config/db_creds_local.yaml")
        engine: Engine = create_engine(
            f"postgresql+psycopg2://{creds['LOCALHOST_USER']}:{creds['LOCALHOST_PASSWORD']}@{creds['LOCALHOST_HOST']}:{creds['LOCALHOST_PORT']}/{creds['LOCALHOST_DATABASE']}"
        )
        alter_query = f"ALTER TABLE {child_table}\n\t"
        alter_query += f"ADD CONSTRAINT {parent_table}_{child_table}_fk\n\t"
        alter_query += (
            f"FOREIGN KEY ({fk_column}) REFERENCES {parent_table} ({pk_column});"
        )
        print(alter_query)
        self.commit_db(engine=engine, query=alter_query)

    def update_datatypes_db(
        self,
        table_name: str,
        column_types: List[Union[str, int]] = None,
    ):
        """
        Upload the Dataframe data to localhost databascrede.

        Parameters:
            - df: DataFrame
            - table_name: string
            - column_types: Dict
            - is_constraints: boolean
        """
        creds: Dict = self.read_db_creds("config/db_creds_local.yaml")
        engine: Engine = create_engine(
            f"postgresql+psycopg2://{creds['LOCALHOST_USER']}:{creds['LOCALHOST_PASSWORD']}@{creds['LOCALHOST_HOST']}:{creds['LOCALHOST_PORT']}/{creds['LOCALHOST_DATABASE']}"
        )
        update_query: str = f"ALTER TABLE {table_name}\n"
        for column in column_types:
            datatype: str = self.__convert_sqlalquemy_data_type_to_string(column[1])
            update_query += f"ALTER COLUMN {column[0]} TYPE {datatype} USING ({column[0]}::{datatype})"
            if len(column) == 3:
                update_query += f"{column[2]}"
            update_query += ",\n"

        update_query = update_query[:-2] + ";"
        print(update_query)
        self.commit_db(engine=engine, query=update_query)
        # try:
        #     uuid_extension = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
        #     with engine.connect() as connection:
        #         connection.execute(text(uuid_extension))
        #         connection.execute(text(update_query))
        #         connection.commit()
        # except exc.ProgrammingError as e:
        #     print(e)

    def create_new_column(self, table_name: str, columns: List[Union[str]]):
        """
        Parameters:
            - table_name: string
            - columns: List
        """
        creds: Dict = self.read_db_creds("config/db_creds_local.yaml")
        engine: Engine = create_engine(
            f"postgresql+psycopg2://{creds['LOCALHOST_USER']}:{creds['LOCALHOST_PASSWORD']}@{creds['LOCALHOST_HOST']}:{creds['LOCALHOST_PORT']}/{creds['LOCALHOST_DATABASE']}"
        )
        for column in columns:
            datatype: str = self.__convert_sqlalquemy_data_type_to_string(column[1])
            new_column_query: str = f"ALTER TABLE {table_name}\n"
            new_column_query += f"ADD IF NOT EXISTS {column[0]} {datatype}"
            self.commit_db(engine=engine, query=new_column_query)
            # try:
            #     uuid_extension = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
            #     with engine.connect() as connection:
            #         connection.execute(text(uuid_extension))
            #         connection.execute(text(new_column))
            #         connection.commit()
            # except exc.ProgrammingError as e:
            #     print(e)

    def select_db(self, select_query: str):
        """
        Parameters:
            - select_query: string
        """

        df: DataFrame = pd.read_sql(select_query, con=self.init_local_db_engine())
        return df
        # connection.commit()

    def insert_db(self, insert_query):
        """
        Parameters:
            - insert_query: string
        """
        engine: Engine = self.init_local_db_engine()
        self.commit_db(engine=engine, query=insert_query)
        # try:
        #     # uuid_extension = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
        #     with engine.connect() as connection:
        #         # connection.execute(text(uuid_extension))
        #         connection.execute(text(insert_query))
        #         connection.commit()
        # except exc.ProgrammingError as e:
        #     print(e)

    def rename_column_name(
        self, table_name: str, column_name: str, new_column_name: str
    ):
        """
        Parameters:
            - table_name: str
            - old_name: str
            - new_name: str
        """
        engine: Engine = self.init_local_db_engine()
        alter_query = f"ALTER TABLE IF EXISTS {table_name} RENAME COLUMN {column_name} TO {new_column_name};"
        self.commit_db(engine=engine, query=alter_query)
        # try:
        #     with engine.connect() as connection:
        #         connection.execute(text(alter_query))
        #         connection.commit()
        # except exc.ProgrammingError as e:
        #     print(e)
