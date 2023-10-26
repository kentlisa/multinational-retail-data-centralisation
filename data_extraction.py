from database_utils import DatabaseConnector as db_conn
import pandas as pd
from tabula import read_pdf
import requests

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
    def retrieve_stores_data(self, store_endpoint, headers):
        # store_data_list = list()
        response = requests.get(store_endpoint, headers = headers)
        # store_data_list.append(response)
        # store_df = pd.DataFrame(store_data_list)
        return response
        
print(requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}))