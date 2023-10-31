from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
import numpy as np


class DataCleaning:
    @classmethod
    def clean_user_data(self):
        ''' This method extracts and cleans the user data.

        Returns
        -------
        user_data : pandas dataframe
            Dataframe containing the cleaned user data.
        '''
        # reads in user data
        user_data = extractor.read_rds_table('legacy_users')

        # converts date columns to datetime dtype
        user_data.date_of_birth = pd.to_datetime(user_data.date_of_birth, yearfirst = True, format = 'mixed', errors = 'coerce')
        user_data.join_date = pd.to_datetime(user_data.join_date, yearfirst = True, format = 'mixed', errors = 'coerce')
        
        # converts country and country code to category dtype
        user_data.country = user_data.country.astype('category')
        user_data.country_code = user_data.country_code.astype('category')

        # censures email address are in correct format
        regex_expression = '^.+@[^\.].*\.[a-z]{2,}$'
        user_data.loc[~user_data.email_address.str.match(regex_expression), 'email_address'] = np.nan
        
        # formats phone numbers, removes excess characters
        user_data.phone_number = user_data.phone_number.astype('string')
        user_data.phone_number = user_data.phone_number.str.replace('.', '')
        user_data.phone_number = user_data.phone_number.str.replace('(0)', '')
        user_data.phone_number = user_data.phone_number.str.replace(')', '')
        user_data.phone_number = user_data.phone_number.str.replace('(', '')
        user_data.phone_number = user_data.phone_number.str.replace(' ', '')
        user_data.phone_number = user_data.phone_number.str.replace('+44', '0')
        user_data.phone_number = user_data.phone_number.str.replace('+49', '0')
        user_data.phone_number = user_data.phone_number.str.replace('+1-', '')
        user_data.phone_number = user_data.phone_number.str.replace('-', '')

        # removes rows with null date of birth, removes duplicates
        user_data.dropna(subset = 'date_of_birth', inplace = True)
        user_data.drop_duplicates(inplace = True)
        user_data.drop(columns = 'index', inplace = True)

        return user_data
    
    @classmethod
    def clean_card_data(self):
        ''' This method extracts and cleans the card data.

        Returns
        -------
        card_data : pandas dataframe
            Dataframe containing cleaned card data.
        '''
        # retrieves data from pdf source
        card_data = DataExtractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

        # drops duplicates
        card_data.drop_duplicates(inplace = True)

        # drops rows with null values
        card_data.dropna(inplace = True)

        # remove stray characters from card numbers
        card_data.card_number = card_data.card_number.astype('string')
        card_data.card_number = card_data.card_number.str.replace('?','')

        # drops incorrect rows
        card_data.card_number = pd.to_numeric(card_data.card_number, errors = 'coerce')
        card_data.dropna(inplace = True)

        return card_data
    
    @classmethod
    def clean_store_data(self):
        ''' This method extracts and cleans the store data.
        
        Returns
        -------
        store_data : pandas dataframe
            Dataframe containing cleaned store data.
        '''
        # creates list of store enpoints
        store_endpoints = list()
        header_details = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        for store_number in range(0,451):
            store_endpoints.append(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}')

        # creates dataframe
        store_data = DataExtractor.retrieve_stores_data(store_endpoints, header_details)
        store_data.set_index('index')

        # removes duplicate rows
        store_data.drop_duplicates()

        # removes rows with incorrect data
        bad_indices = [63, 172, 414, 381, 217, 405, 437, 231, 447, 333]
        store_data.drop(index = bad_indices, inplace = True)

        # removes stray characters from continent
        store_data.continent = store_data.continent.str.replace('ee', '')

        # sets category columns
        store_data.locality = store_data.locality.astype('category')
        store_data.store_type = store_data.store_type.astype('category')
        store_data.country_code = store_data.country_code.astype('category')
        store_data.continent = store_data.continent.astype('category')

        # removes alphabetic characters from staff numbers
        # sets to numeric
        store_data.staff_numbers = store_data.staff_numbers.str.extract('(\d+)', expand=False)
        store_data.staff_numbers = pd.to_numeric(store_data.staff_numbers, errors = 'coerce')

        # drop index column
        store_data.drop(columns = 'index', inplace = True)

        return store_data
    
    @classmethod
    def convert_product_weights(self, product_data):
        ''' This method converts the weight column of the product dataframe to a uniform format.
        
        Parameters
        ----------
        product_data : pandas dataframe
            Dataframe containing product data.
        
        Returns
        -------
        product_data : pandas dataframe
            Dataframe containing product data with the weight column in uniform format.
        '''
        # converts column to string
        product_data.weight = product_data.weight.astype('string')

        # masks entries that need multiplying
        mask = product_data.weight.str.contains('x')
        multi_df = product_data.weight[mask]
        multi_indices = list(multi_df.index)

        # remove units
        multi_df = multi_df.str.replace('g', '')

        # split into number of items and weight per item columns
        multi_df = multi_df.str.split(' x ', expand = True)
        col_names = {0: 'number', 1: 'weight'}
        multi_df.rename(columns = col_names, inplace = True)
        multi_df.number = pd.to_numeric(multi_df.number)
        multi_df.weight = pd.to_numeric(multi_df.weight)

        # multiply to form total weight
        multi_df['total_weight'] = multi_df.number * multi_df.weight
        multi_df.total_weight = multi_df.total_weight.astype('string')

        # #replaces weight values in main df
        for index, new_index in zip(multi_indices, range(0, len(multi_df.total_weight))):
            product_data.weight.iloc[index] = multi_df.total_weight.iloc[new_index]

        # removes units from grams and mls
        product_data.weight = product_data.weight.str.replace('ml', '')
        product_data.weight = product_data.weight.str.replace('g', '')

        # #mask out kgs
        mask_kg = product_data.weight.str.contains('k')
        kg_df = pd.DataFrame()
        kg_df['weight'] = product_data.weight[mask_kg]
        kg_indices = list(kg_df.index)

        # remove k
        kg_df.weight = kg_df.weight.str.replace('k', '')

        #convert to grams
        kg_df.weight = pd.to_numeric(kg_df.weight)
        kg_df.weight = kg_df.weight * 1000

        #convert back to string column
        kg_df = kg_df.weight.astype('string')

        #replace values in main dataframe
        for index, new_index in zip(kg_indices, range(0, 954)):
            product_data.weight[index] = kg_df.iloc[new_index]

        #set weight to numeric column
        product_data.weight = pd.to_numeric(product_data.weight, errors = 'coerce')

        #all entries now in grams, converts to kgs
        product_data.weight = product_data.weight / 1000
        
        return product_data

    @classmethod
    def clean_products_data(self):
        ''' This method extracts and cleans the product data.

        Returns
        -------
        product_data : pandas dataframe
            Dataframe containing cleaned product data.
        '''
        # extracts data from s3 bucket, converts to dataframe
        product_data = extractor.extract_from_s3('data-handling-public','products.csv')
        product_data = self.convert_product_weights(product_data)

        #drop duplicates
        product_data.drop_duplicates(inplace = True)

        #drop nulls
        product_data.dropna(inplace = True)

        #drop index column
        product_data = product_data.loc[:, ~product_data.columns.str.contains('^Unnamed')]

        return product_data
    
    @classmethod
    def clean_orders_data(self):
        ''' This method extracts and cleans the order data.
        
        Returns
        -------
        order_data : pandas dataframe
            Dataframe containing the cleaned order data.
        '''
        # extracts order data
        order_data = extractor.read_rds_table('orders_table')

        # drops unnecesary columns
        order_data.drop(columns = ['first_name', 'last_name', '1', 'level_0', 'index'], inplace = True)

        return order_data
    
    @classmethod
    def clean_date_details(self):
        ''' This method extracts and cleans the date details.
        
        Returns 
        -------
        date_details : pandas dataframe
            Dataframe containing the cleaned date details.
        '''
        # extract json data into dataframe
        date_details = extractor.extract_from_s3('data-handling-public','date_details.json')

        # filters incorrect rows to null in month
        date_details.month = pd.to_numeric(date_details.month, errors = 'coerce')

        # drops fully null rows and incorrect rows
        date_details.dropna(inplace = True)

        return date_details


# extracts number of stores
extractor = DataExtractor()
# number_of_stores = extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})

# cleans all data
cleaner = DataCleaning
# cleaned_user_data = cleaner.clean_user_data()
cleaned_card_data = cleaner.clean_card_data()
# cleaned_store_data = cleaner.clean_store_data()
# cleaned_product_data = cleaner.clean_products_data()
# cleaned_order_data = cleaner.clean_orders_data()
# cleaned_date_details = cleaner.clean_date_details()

# uploads to database
connector = DatabaseConnector
# connector.list_db_tables()
# connector.upload_to_db(cleaned_user_data, 'dim_users')
connector.upload_to_db(cleaned_card_data, 'dim_card_details')
# connector.upload_to_db(cleaned_store_data, 'dim_store_details')
# connector.upload_to_db(cleaned_product_data, 'dim_products')
# connector.upload_to_db(cleaned_order_data, 'orders_table')
# connector.upload_to_db(cleaned_date_details, 'dim_date_times')
