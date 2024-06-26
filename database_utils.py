import yaml

from sqlalchemy import create_engine, text
from sqlalchemy import UUID


class DatabaseConnector:

    def __create_dim_table(self, connection, df):
        """
        Create dim table in repository with hardcode query.
        The df.to_sql creates the table and insert data automatically, making this function unnecessary.
        """
        create_table = f"CREATE TABLE IF NOT EXISTS dim_users (\n"
        for type, column_name in zip(df.dtypes, df):
            if column_name == "index":
                create_table += f"\t\t{column_name} integer NOT NULL PRIMARY KEY,\n"
            elif column_name == "user_uuid":
                create_table += (
                    f"\t\t{column_name} UUID DEFAULT uuid_generate_v4(),\n"
                    # f"\t\t{column_name} UUID NOT NULL DEFAULT uuid_generate_v4(),\n"
                )
            elif type == "string":
                create_table += f"\t\t{column_name} varchar(40),\n"
            elif type == "datetime64[ns]":
                create_table += f"\t\t{column_name} date,\n"
        create_table = create_table[:-2] + "\n);"

        uuid_extension = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
        connection.execute(text(uuid_extension))
        connection.execute(text(create_table))
        connection.commit()

    def read_db_creds(self, filename):
        """
        Read credentials DB
        Parameters:
            filename: int
        """
        with open(filename, "r") as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self):
        """
        Create a engine for postgress DataBase
        """
        creds = self.read_db_creds("config/db_creds.yaml")
        # Database URLs sintax: dialect+driver://username:password@host:port/database
        return create_engine(
            f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )

    def upload_to_db(self, df, table_name, column_types=None):
        """
        Upload the Dataframe data to localhost databascrede.
        """
        creds = self.read_db_creds("config/db_creds_local.yaml")
        engine = create_engine(
            f"postgresql+psycopg2://{creds['LOCALHOST_USER']}:{creds['LOCALHOST_PASSWORD']}@{creds['LOCALHOST_HOST']}:{creds['LOCALHOST_PORT']}/{creds['LOCALHOST_DATABASE']}"
        )
        df.to_sql(table_name, con=engine, if_exists="replace", dtype=column_types)
