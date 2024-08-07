import re
from typing import Dict, List

from sqlalchemy import Engine, create_engine, text, exc
import yaml


class BaseDatabaseConnector:
    __DB_USER: str = "USER"
    __DB_PASSWORD: str = "PASSWORD"
    __DB_HOST: str = "HOST"
    __DB_PORT: str = "PORT"
    __DB_DATABASE: str = "DATABASE"

    __SMALLINT: str = "SMALLINT"
    __SMALL_INTEGER: str = "small_integer"
    __VARCHAR: str = "VARCHAR"

    # It will be used as a index in the list
    _COLUMN_NAME: int = 0
    _COLUMN_TYPE: int = 1

    def _read_db_creds(self, filename: str) -> Dict:
        """
        Read credentials DB
        * Parameters:
            - filename: str
        """
        with open(filename, "r") as file:
            creds: Dict = yaml.safe_load(file)
        return creds

    def _create_engine(self, is_localhost: bool = True) -> Engine:
        """ """
        creds: Dict = {}
        if not is_localhost:
            creds = self._read_db_creds("config/db_creds.yaml")
        else:
            creds = self._read_db_creds("config/db_creds_local.yaml")

        # Database URLs sintax: dialect+driver://username:password@host:port/database
        engine: Engine = create_engine(
            f"postgresql+psycopg2://{creds[self.__DB_USER]}:{creds[self.__DB_PASSWORD]}@{creds[self.__DB_HOST]}:{creds[self.__DB_PORT]}/{creds[self.__DB_DATABASE]}"
        )
        return engine

    def _convert_sqlalquemy_data_type_to_string(self, column: str) -> str:
        if column.__visit_name__ == self.__SMALL_INTEGER:
            return self.__SMALLINT
        elif column.__visit_name__ == self.__VARCHAR:
            length = column.__getattribute__("length")
            return f"{column.__visit_name__}({length})"
        else:
            return column.__visit_name__

    def _create_table(self, engine, table_name: str, columns: List[str]):
        """
        Create dim table in repository with hardcode query.
        The df.to_sql creates the table and insert data automatically, making this function unnecessary.

        * Parameters:
            - engine:
            - table_name: string
            - columns: string
        """
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        for column in columns:
            column[self._COLUMN_TYPE] = self._convert_sqlalquemy_data_type_to_string(
                column[self._COLUMN_TYPE]
            )
            for attribute in column:
                create_query += f"\t\t{ attribute}"
            create_query += f",\n"

        create_query = create_query[:-2] + "\n);"
        self.commit_db(engine=engine, query=create_query)

    def _drop_foreign_key(self, engine: Engine, table_name: str):
        """
        * Parameters:
            - engine: Engine
            - table_name: string
        """
        delete_query: str = (
            f"ALTER TABLE orders_table DROP CONSTRAINT {table_name}_orders_table_fk;"
        )
        self.commit_db(engine=engine, query=delete_query)

    def _drop_table(self, engine: Engine, table_name: str):
        """
        * Parameters:
            - engine: Engine
            - table_name: string
        """
        delete_query = f"DROP TABLE {table_name} CASCADE;"
        self.commit_db(engine=engine, query=delete_query)

    def _insert_data(self, engine, table_name, df):
        columns = tuple(df.columns.to_list())
        insert_query = f"INSERT INTO {table_name} "
        insert_query += f"{columns} VALUES\n("
        for column in columns:
            insert_query += f":{column}, "
        insert_query = insert_query[:-2] + ");"
        insert_query = re.sub(r"'", "", insert_query)
        print(insert_query)
        data = df.to_dict(orient="records")
        self.commit_db(engine=engine, query=insert_query, data=data)

    def commit_db(
        self, engine: Engine, query: str, data: List = None, output: bool = True
    ):
        """
        * Parameters:
            - engine: Engine
            - query: string
        """
        try:
            if output:
                print(query)
            uuid_extension = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
            with engine.connect() as connection:
                connection.execute(text(uuid_extension))
                connection.execute(text(query), data)
                connection.commit()
        except exc.ProgrammingError as e:
            print(e)
        except exc.IntegrityError as e:
            print(e)
        except exc.InternalError as e:
            print(e)
