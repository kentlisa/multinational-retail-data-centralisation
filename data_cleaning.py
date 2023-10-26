from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd


class DataCleaning:
    @classmethod
    def clean_user_data(self):
        user_data = DataExtractor.read_rds_table()

        # converts date columns to datetime dtype
        user_data.date_of_birth = pd.to_datetime(user_data.date_of_birth, yearfirst = True, format = 'mixed', errors = 'coerce')
        user_data.join_date = pd.to_datetime(user_data.join_date, yearfirst = True, format = 'mixed', errors = 'coerce')
        
        # convert country and country code to category dtype
        # conserves memory
        user_data.country = user_data.country.astype('category')
        user_data.country_code = user_data.country_code.astype('category')
        
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

        #removes rows with null date of birth and join date
        user_data.dropna(subset = 'date_of_birth', inplace = True)

        return user_data
    
    @classmethod
    def clean_card_data(self):
        card_data = DataExtractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        card_data.drop_duplicates()
        card_data.card_number = pd.to_numeric(card_data.card_number, errors = 'coerce')
        card_data.card_provider = card_data.card_provider.astype('category')
        card_data.expiry_date = card_data.expiry_date.astype('category')
        card_data.date_payment_confirmed = pd.to_datetime(card_data.date_payment_confirmed, format = 'mixed', yearfirst = True, errors = 'coerce')
        card_data.dropna(inplace = True)
        print(card_data.info())
        return card_data
    
    # @classmethod
    # def clean_store_data(self):


cleaner = DataCleaning
#cleaned_user_data = cleaner.clean_user_data()
#cleaned_card_data = cleaner.clean_card_data()


connector = DatabaseConnector
#connector.upload_to_db(cleaned_user_data, 'dim_users')
#connector.upload_to_db(cleaned_card_data, 'dim_card_details', index = True)

# api details
header_details = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
number_stores_endp = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

extractor = DataExtractor
number_of_stores = extractor.list_number_of_stores(number_stores_endp, header_details)
print(number_of_stores)

#store endpoint list
# store_endpoints = list()
# for store_number in range(0,3):
#     store_endpoints.append(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores{store_number}')

store_data = extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores/2', header_details)
print(store_data)

