import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect


class DatabaseConnector:
    @classmethod
    def read_db_creds(self):
        with open('.gitignore/db_creds.yaml','r') as file:
                try:
                    creds = yaml.safe_load(file)
                    return creds
                except yaml.YAMLError as e:
                     print(e)

    @classmethod
    def init_db_engine(self):
        creds_dict = self.read_db_creds()
        creds_list = list(creds_dict.values())
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds_list[0]
        PASSWORD = creds_list[1]
        USER = creds_list[2]
        DATABASE = creds_list[3]
        PORT = creds_list[4]
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
    
    @classmethod
    def list_db_tables(self):
        self.engine = self.init_db_engine()
        inspector = inspect(self.engine)
        self.table_names = inspector.get_table_names()
        print(self.table_names)
        return self.table_names

    @classmethod
    def upload_to_db(self, data_frame, table_name, index = False):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        PASSWORD = 'emmalisa'
        USER = 'postgres'
        DATABASE = 'sales_data'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        data_frame.to_sql(name = table_name, con = engine, if_exists = 'replace', index = index)

