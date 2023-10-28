from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
import numpy as np


class DataCleaning:
    @classmethod
    def clean_user_data(self):
        user_data = extractor.read_rds_table('legacy_users')

        # converts date columns to datetime dtype
        user_data.date_of_birth = pd.to_datetime(user_data.date_of_birth, yearfirst = True, format = 'mixed', errors = 'coerce')
        user_data.join_date = pd.to_datetime(user_data.join_date, yearfirst = True, format = 'mixed', errors = 'coerce')
        
        # converts country and country code to category dtype
        # conserves memory
        user_data.country = user_data.country.astype('category')
        user_data.country_code = user_data.country_code.astype('category')

        #ensures email address are in correct format
        regex_expression = '^.+@[^\.].*\.[a-z]{2,}$'
        user_data.loc[~user_data.email_address.str.match(regex_expression), 'email_address'] = np.nan
        
        # formats phone numbers
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

        # converts all other columns to string dtype
        user_data.first_name = user_data.first_name.astype('string')  
        user_data.last_name = user_data.last_name.astype('string')  
        user_data.company = user_data.company.astype('string')  
        user_data.email_address = user_data.email_address.astype('string')  
        user_data.address = user_data.address.astype('string')  
        user_data.user_uuid = user_data.user_uuid.astype('string')

        #removes rows with null date of birth or email
        user_data.dropna(subset = 'date_of_birth', inplace = True)
        user_data.dropna(subset = 'email_address', inplace = True)
        user_data.drop_duplicates(inplace = True)

        return user_data
    
    @classmethod
    def clean_card_data(self):
        card_data = DataExtractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        card_data.drop_duplicates(inplace = True)
        card_data.card_number = pd.to_numeric(card_data.card_number, errors = 'coerce')
        card_data.card_provider = card_data.card_provider.astype('category')
        card_data.expiry_date = card_data.expiry_date.astype('category')
        card_data.date_payment_confirmed = pd.to_datetime(card_data.date_payment_confirmed, format = 'mixed', yearfirst = True, errors = 'coerce')
        card_data.dropna(inplace = True)

        return card_data
    
    @classmethod
    def clean_store_data(self):
        store_endpoints = list()
        header_details = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        for store_number in range(1,451):
            store_endpoints.append(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}')
        store_data = DataExtractor.retrieve_stores_data(store_endpoints, header_details)
        store_data.set_index('index')

        #removes duplicate rows
        store_data.drop_duplicates()

        #removes rows with incorrect data
        bad_indices = [62, 171, 413, 380, 216, 404, 436, 230, 446, 332]
        store_data.drop(index = bad_indices, inplace = True)

        #removes lat column
        store_data.drop(columns = 'lat', inplace = True)

        #removes stray characters from continent
        store_data.continent = store_data.continent.str.replace('ee', '')

        #sets category columns
        store_data.locality = store_data.locality.astype('category')
        store_data.store_type = store_data.store_type.astype('category')
        store_data.country_code = store_data.country_code.astype('category')
        store_data.continent = store_data.continent.astype('category')

        #sets numeric columns
        store_data.longitude = pd.to_numeric(store_data.longitude, errors = 'coerce')
        store_data.latitude = pd.to_numeric(store_data.latitude, errors = 'coerce')

        #removes alphabetic characters from staff numbers
        #sets to numeric
        store_data.staff_numbers = store_data.staff_numbers.str.extract('(\d+)', expand=False)
        store_data.staff_numbers = pd.to_numeric(store_data.staff_numbers, errors = 'coerce')

        #sets string columns
        store_data.address = store_data.address.astype('string')
        store_data.store_code = store_data.store_code.astype('string')

        #sets date column
        store_data.opening_date = pd.to_datetime(store_data.opening_date, errors = 'coerce', format = 'mixed', yearfirst = True)

        store_data.info()
        return store_data
    
    @classmethod
    def convert_product_weights(self, df):
        #converts column to string
        df.weight = df.weight.astype('string')

        #masks entries that need multiplying
        mask_multiples = df.weight.str.contains('x')
        multi_df = df.weight[mask_multiples]
        multi_indices = list(multi_df.index)

        #remove units
        multi_df = multi_df.str.replace('g', '')

        #split into number of items and weight per item columns
        multi_df = multi_df.str.split(' x ', expand = True)
        col_names = {0: 'number', 1: 'weight'}
        multi_df.rename(columns = col_names, inplace = True)
        multi_df.number = pd.to_numeric(multi_df.number)
        multi_df.weight = pd.to_numeric(multi_df.weight)

        #creates total weight column
        multi_df['total_weight']= multi_df.number * multi_df.weight
        multi_df.total_weight = multi_df.total_weight.astype('string')

        #replaces weight values in main df
        for index, new_index in zip(multi_indices, range(0, len(multi_df.total_weight))):
            df.weight.iloc[index] = multi_df.total_weight.iloc[new_index]

        #removes units from grams and mls
        df.weight = df.weight.str.replace('ml', '')
        df.weight = df.weight.str.replace('g', '')

        #mask out kgs
        mask_kg = df.weight.str.contains('k')
        kg_df = pd.DataFrame()
        kg_df['weight'] = df.weight[mask_kg]
        kg_indices = list(kg_df.index)

        #remove k
        kg_df.weight = kg_df.weight.str.replace('k', '')

        #convert to grams
        kg_df.weight = pd.to_numeric(kg_df.weight)
        kg_df.weight = kg_df.weight * 1000

        #convert back to string column
        kg_df = kg_df.weight.astype('string')

        #replace values in main df
        for index, new_index in zip(kg_indices, range(0, 954)):
            df.weight.iloc[index] = kg_df.iloc[new_index]

        #set weight to numeric column
        df.weight = pd.to_numeric(df.weight, errors = 'coerce')

        #all entries now in grams, converts to kgs
        df.weight = df.weight / 1000
        
        return df

    @classmethod
    def clean_products_data(self):
        df = extractor.extract_from_s3('data-handling-public','products.csv')
        df.set_index(pd.Index(range(0,1853)))
        df = self.convert_product_weights(df)

        #drop duplicates
        df.drop_duplicates(inplace = True)

        #drop nulls
        df.dropna(inplace = True)

        return df
    
    @classmethod
    def clean_orders_data(self):
        order_data = extractor.read_rds_table('orders_table')

        #drop unnecesary columns
        order_data.drop(columns = ['first_name', 'last_name', '1', 'level_0'], inplace = True)

        return order_data
    
    @classmethod
    def clean_date_details(self):
        #extract json data into dataframe
        df = extractor.extract_from_s3('data-handling-public','date_details.json')

        #create adn set index column
        df['index'] = pd.Index(range(0,len(df.month)))
        df.set_index('index')

        return df


extractor = DataExtractor
# number_of_stores = extractor.list_number_of_stores(number_stores_endp, header_details)
# print(number_of_stores)
# print(product_data.info())
# print(product_data.weight)

cleaner = DataCleaning
# cleaned_user_data = cleaner.clean_user_data()
# cleaned_card_data = cleaner.clean_card_data()
# cleaned_store_data = cleaner.clean_store_data()
# product_data = cleaner.convert_product_weights(product_data)
# product_data = cleaner.clean_products_data()
# order_data = cleaner.clean_orders_data()
date_details = cleaner.clean_date_details()
# print(product_data.info())

connector = DatabaseConnector
# connector.list_db_tables()
# connector.upload_to_db(cleaned_user_data, 'dim_users')
# connector.upload_to_db(cleaned_card_data, 'dim_card_details', index = True)
# connector.upload_to_db(cleaned_store_data, 'dim_store_details')
# connector.upload_to_db(product_data, 'dim_products')
# connector.upload_to_db(order_data, 'orders_table')
connector.upload_to_db(date_details, 'dim_date_times')


# api details
header_details = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
number_stores_endp = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
