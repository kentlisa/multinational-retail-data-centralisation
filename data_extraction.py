from database_utils import DatabaseConnector as db_conn
import pandas as pd
from tabula import read_pdf
import requests
import boto3

class DataExtractor:
    @classmethod
    def read_rds_table(self):
        db_conn.list_db_tables()
        user_data = pd.read_sql_table('legacy_users', db_conn.engine)
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
    def extract_from_s3(self, address):
        s3 = boto3.client('s3')
        split_address = address.split('/')
        s3.download_file(split_address[2], split_address[3], 'products.csv')
        product_data = pd.read_csv('products.csv', index_col= 0)
        return product_data
        
