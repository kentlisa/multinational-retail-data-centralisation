from database_utils import DatabaseConnector as db_conn
import pandas as pd
from tabula import read_pdf
import requests
import boto3

class DataExtractor:
    @classmethod
    def read_rds_table(self, table_name):
        engine = db_conn.init_db_engine()
        user_data = pd.read_sql_table(table_name, engine)
        pd.set_option('display.max_columns', None)
        user_data.set_index('index')
        return user_data
    
    @classmethod
    def retrieve_pdf_data(self, link):
        card_data = pd.concat(read_pdf(link, pages = 'all', lattice = True, multiple_tables = True))
        return card_data
    
    @classmethod
    def list_number_of_stores(self, endpoint, headers):
        response = requests.get(endpoint, headers=headers)
        return response.text
    
    @classmethod
    def retrieve_stores_data(self, store_endpoints : list, headers):
        store_data_list = list()
        for endpoint in store_endpoints:
            response = requests.get(endpoint, headers = headers)
            store_data_list.append(response.json())
        store_df = pd.DataFrame(store_data_list)       
        return store_df
    
    @classmethod
    def extract_from_s3(self, bucket, file):
        s3 = boto3.client('s3')
        s3.download_file(bucket, file, file)
        if 'csv' in file:
            data = pd.read_csv(file)
        elif 'json' in file:
            data = pd.read_json(file)
        else:
            print('unknown file type')
        return data
