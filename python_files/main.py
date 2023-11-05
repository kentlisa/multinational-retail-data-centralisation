from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector


# extracts number of stores
extractor = DataExtractor()
number_of_stores = extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'})

# cleans all data
cleaner = DataCleaning
cleaned_user_data = cleaner.clean_user_data()
cleaned_card_data = cleaner.clean_card_data()
cleaned_store_data = cleaner.clean_store_data()
cleaned_product_data = cleaner.clean_products_data()
cleaned_order_data = cleaner.clean_orders_data()
cleaned_date_details = cleaner.clean_date_details()

# uploads to database
connector = DatabaseConnector
connector.list_db_tables()
# connector.upload_to_db(cleaned_user_data, 'dim_users')
# connector.upload_to_db(cleaned_card_data, 'dim_card_details')
# connector.upload_to_db(cleaned_store_data, 'dim_store_details')
# connector.upload_to_db(cleaned_product_data, 'dim_products')
# connector.upload_to_db(cleaned_order_data, 'orders_table')
# connector.upload_to_db(cleaned_date_details, 'dim_date_times')
