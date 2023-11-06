# Multinational Retail Data Centralisation
---
The project begins with a large retail corporation whose data is disorganised and difficult to manage. This project brings all the data into one place, in a concise manner.\
It puts into practise my skills in OOP, with three classes that perform operations on the data and the database. Complex SQL queries were used to perform analysis on the final database, including the use of subqueries, joins and CTEs.


## Table of Contents

- [1. The Code](#1-the-code)
   * [1.1. Database Connector](#11-database-connector)
   * [1.2. Data Extractor](#12-data-extractor)
   * [1.3. Data Cleaning](#13-data-cleaning)
   * [1.4. SQL](#14-sql)
- [2. Dependencies](#2-dependencies)
- [3. Installation](#3-installation)
- [4. Usage](#4-usage)
- [5. File Structure](#5-file-structure)

## 1. The Code
There are 3 classes in this project:
- **DatabaseConnector** is in ```database_utils.py``` ;  it contains methods that interact with the database
- **DatabaseExtractor** is in ```data_extraction.py``` ;  it contains methods that extract data from various sources. 
- **DataCleaning** is in ```data_cleaning.py``` ; it contains methods that clean the various data.

```main.py``` initialises the class and runs the project.

### 1.1. Database Connector

- ``` read_db_creds``` gathers and returns the database credentials from the yaml file
- ```init_db_engine``` connects to the new database, returning a sqlalchemy engine
- ```list_db_tables``` prints the table names from the intial database
- ```upload_to_db``` takes in a pandas dataframe and uploads its contents to the new database

### 1.2. Data Extractor

- ```read_rds_table``` retrieves data from the intial database and returns it in a pandas dataframe
- ```retrieve_pdf_data``` gathers pdf data from a given url and returns a pandas dataframe
- ```list_number_of_stores``` performs a get request to a given url, in this case to return the number of stores to gather data about
- ```retrieve_stores_data``` performs get requests to gather the data about each store, and combines them into a single pandas dataframe
- ```extract_from_s3``` extracts data from a given file stored in an s3 bucket and returns a pandas dataframe

### 1.3. Data Cleaning

- ```clean_user_data``` cleans the user data, by formatting columns using regex and removing stray characters
- ```clean_card_data``` cleans the card data by removing stray characters and null values
- ```clean_store_data``` cleans the store data by removing stray rows of incorrect data, formatting columns using regex and removing stray characters
- ```convert_product_weights``` formats the weight column in the product table, converting all the weights to kilograms
- ```clean_product_data``` cleans the product data by calling ```convert product weights```, and removing null values
- ```clean_orders_data``` cleans the orders data by removing unnecessary columns
- ```clean_date_details``` cleans the date details by removing null values

### 1.4. SQL
The SQL files have been split into two for clarity.

```data_formatting.sql``` contains the column type casting and the creation of the primary and foreign keys.

```data_info.sql``` contains the queries that extract data required to answer the set quetions. The questions are as follows:
1. How many stores does the business have and in which countries?
2. Which locations currently have the most stores?
3. Which months produce the average highest cost of sales?
4. How many sales are coming from online?
5. What percentage of sales come through each type of store?
6. Which month in each year produced the highest cost of sales?
7. What is the staff headcount?
8. Which German store type is selling the most?
9. How quickly is the company making sales?

## 2. Dependencies
Install the following python packages before running the project:

- pandas
- numpy
- sqlalchemy
- pyyaml (yaml)
- tabula
- requests
- boto3

## 3. Installation
To begin the project, clone the repo by running the following line in your terminal:

```
git clone https://github.com/kentlisa/multinational-retail-data-centralisation.git
```

Or, if you have GitHub CLI then run the following:

```
gh repo clone kentlisa/multinational-retail-data-centralisation
```

## 4. Usage

Begin by running the python element of the project by running:
```
main.py
``` 
This gathers all the data, cleans it and uploads to the database.

Then run:
 ```
 data_formatting.sql
 ```
 This casts the data types for all columns, then sets up the primary and foreign keys.

 Followed by:
 ```
 data_info.sql
 ``` 
 This will present the solutions to the nine questions outlined in Section 1.4.

## 5. File Structure
```
├── python_files
│   ├── data_cleaning.py
│   ├── data_extraction.py
│   ├── database_utils.py
│   └── main.py
├── raw_data_files
│   ├── date_details.json
│   └── products.csv
├── sql_files
│   ├── data_formatting.sql
│   └── data_info.sql
└── README.md
```
