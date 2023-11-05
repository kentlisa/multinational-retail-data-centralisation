-- orders table column type casting

ALTER TABLE orders_table
ALTER COLUMN date_uuid 
TYPE uuid 
USING date_uuid::uuid;

ALTER TABLE orders_table
ALTER COLUMN user_uuid 
TYPE uuid 
USING user_uuid::uuid;

ALTER TABLE orders_table
ALTER COLUMN card_number
TYPE VARCHAR(19);

ALTER TABLE orders_table
ALTER COLUMN store_code
TYPE VARCHAR(12);

ALTER TABLE orders_table
ALTER COLUMN product_code
TYPE VARCHAR(11);

ALTER TABLE orders_table
ALTER COLUMN product_quantity 
TYPE SMALLINT;


-- users table column type casting

ALTER TABLE dim_users
ALTER COLUMN first_name
TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN last_name
TYPE VARCHAR(255);

ALTER TABLE dim_users
ALTER COLUMN date_of_birth
TYPE DATE;

ALTER TABLE dim_users
ALTER COLUMN country_code
TYPE VARCHAR(3);

ALTER TABLE dim_users
ALTER COLUMN user_uuid
TYPE uuid 
USING user_uuid::uuid;

ALTER TABLE dim_users
ALTER COLUMN join_date
TYPE DATE;


-- orders table column type casting

ALTER TABLE dim_store_details
DROP COLUMN lat;

ALTER TABLE dim_store_details
ALTER COLUMN longitude
TYPE FLOAT
USING longitude::FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN locality
TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_code
TYPE VARCHAR(12);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers
TYPE SMALLINT;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date
TYPE DATE
USING opening_date::DATE;

ALTER TABLE dim_store_details
ALTER COLUMN store_type
TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN latitude
TYPE FLOAT
USING latitude::FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN country_code
TYPE VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent
TYPE VARCHAR(255);

-- products table column type casting

UPDATE dim_products
SET product_price = TRIM(product_price, '£');

-- creating weight class column
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14);

UPDATE dim_products
SET weight_class = 
CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 
        AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40
        AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;

ALTER TABLE dim_products
ALTER COLUMN product_price
TYPE FLOAT
USING product_price::FLOAT;

ALTER TABLE dim_products
ALTER COLUMN weight
TYPE FLOAT;

ALTER TABLE dim_products
ALTER COLUMN "EAN"
TYPE VARCHAR(17);

ALTER TABLE dim_products
ALTER COLUMN product_code
TYPE VARCHAR(12);

ALTER TABLE dim_products
ALTER COLUMN date_added
TYPE DATE
USING date_added::DATE;

ALTER TABLE dim_products
ALTER COLUMN uuid
TYPE uuid
USING uuid::uuid;

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
ALTER COLUMN still_available
TYPE BOOLEAN
USING CASE
    WHEN still_available = 'Still_avaliable' then TRUE
    WHEN still_available = 'Removed' then FALSE
END;


-- date details column type casting

ALTER TABLE dim_date_times
ALTER COLUMN month
TYPE VARCHAR(2);

ALTER TABLE dim_date_times
ALTER COLUMN year
TYPE VARCHAR(4);

ALTER TABLE dim_date_times
ALTER COLUMN day
TYPE VARCHAR(2);

ALTER TABLE dim_date_times
ALTER COLUMN month
TYPE VARCHAR(2);

ALTER TABLE dim_date_times
ALTER COLUMN time_period
TYPE VARCHAR(10);

ALTER TABLE dim_date_times
ALTER COLUMN date_uuid
TYPE uuid
USING date_uuid::uuid;


-- card details column type casting

ALTER TABLE dim_card_details
ALTER COLUMN card_number
TYPE VARCHAR(19);

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date
TYPE VARCHAR(5);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed
TYPE DATE
USING date_payment_confirmed::DATE;


-- set primary keys for dim tables

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users 
ADD PRIMARY KEY (user_uuid);


-- add foreign keys to orders table

ALTER TABLE orders_table
ADD CONSTRAINT fk_card_numbers 
FOREIGN KEY (card_number) 
REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuids
FOREIGN KEY (date_uuid) 
REFERENCES dim_date_times (date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_product_codes
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_codes
FOREIGN KEY (store_code) 
REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuids
FOREIGN KEY (user_uuid) 
REFERENCES dim_users (user_uuid);
