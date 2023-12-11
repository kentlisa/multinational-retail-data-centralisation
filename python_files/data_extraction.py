import pandas as pd
from tabula import read_pdf
import requests
import boto3


class DataExtractor:
    @classmethod
    def read_rds_table(cls, table_name, connector):
        ''' This method extracts data from the database
        into a pandas dataframe.

        Parameters
        ----------
        table_name : str
            Name of the table to extract data from.

        Returns
        -------
        df : pandas dataframe
            Data from the chosen table.
        '''
        # connects to database
        engine = connector.init_db_engine()

        # reads into pandas dataframe
        df = pd.read_sql_table(table_name, engine)
        pd.set_option('display.max_columns', None)
        df.set_index('index')
        return df

    @classmethod
    def retrieve_pdf_data(cls, url):
        ''' This method retrieves data from a pdf a url.

        Parameters
        ----------
        url : str
            Link to the pdf document.

        Returns
        -------
        df : pandas dataframe
            Dataframe containing data from pdf source.
        '''
        df = pd.concat(read_pdf(url, pages='all',
                                lattice=True, multiple_tables=True))
        return df

    @classmethod
    def list_number_of_stores(cls, endpoint, headers):
        ''' This method performs a get request from a given url.

        Parameters
        ----------
        endpoint : str
            Endpoint url for data retrieval.
        headers : dict
            Headers for get request.

        Returns
        -------
        response.text : str
            Number of stores in the database.
        '''
        response = requests.get(endpoint, headers=headers)
        print(f'Number of stores: {response.text}')

    @classmethod
    def retrieve_stores_data(cls, store_endpoints: list, headers):
        ''' This method performs a get request to gather the data
        for all the stores and collates them into a dataframe.

        Parameters
        ----------
        store_endpoints : list
            List containing enpoint urls for the requests.
        headers : dict
            Headers for the get requests.

        Returns
        -------
        df : pandas dataframe
            Dataframe containing data collected in the get requests.
        '''
        store_data_list = list()
        # iterate through all urls, returns list of dictionaries
        for endpoint in store_endpoints:
            response = requests.get(endpoint, headers=headers)
            store_data_list.append(response.json())
        # converts list of dictionaries to dataframe
        df = pd.DataFrame(store_data_list)
        return df

    @classmethod
    def extract_from_s3(cls, bucket, file):
        ''' This method extracts data from a file in an AWS s3 bucket.

        Parameters
        ----------
        bucket : str
            Name of the bucket containing the file.
        file : str
            Name of the file to extract data from.

        Returns
        -------
        df : pandas dataframe
            Dataframe containing data extracted from target file.
        '''
        # connects to s3
        s3 = boto3.client('s3')
        s3.download_file(bucket, file, f'raw_data_files/{file}')
        # converts to dataframe for given filetype
        if 'csv' in file:
            df = pd.read_csv(f'raw_data_files/{file}')
        elif 'json' in file:
            df = pd.read_json(f'raw_data_files/{file}')
        else:
            print('Can only extract from json or csv files.')
        return df
