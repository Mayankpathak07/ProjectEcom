import os, pandas as pd
from dotenv import load_dotenv
load_dotenv(".env.local")

cleaned_files = os.listdir(r"C:\Users\Mayank Pathak\Downloads\E - commerce pipeline cleaned data")


l = []
for i in cleaned_files:
    l.append(pd.read_csv(r"C:\Users\Mayank Pathak\Downloads\E - commerce pipeline cleaned data\{}".format(i)))


cleaned_files


olist_customers_cleaned_dataset = l[0]

olist_geolocation_cleaned_dataset = l[1]

olist_orders_cleaned_dataset = l[2]

olist_order_items_cleaned_dataset = l[3]

olist_order_payments_cleaned_dataset = l[4]

olist_order_reviews_cleaned_dataset = l[5]

olist_products_cleaned_dataset = l[6]

olist_sellers_cleaned_dataset = l[7]


olist_orders_cleaned_dataset.info()

data = pd.read_csv("Event_Data.csv")


import pandas as pd
import mysql.connector
from mysql.connector import Error

# MySQL connection setup
try:
    connection = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password= os.getenv("DB_PASSWORD"),
        database= os.getenv("DB_NAME")
    )
    if connection.is_connected():
        cursor = connection.cursor()
        print('Connected to MySQL database')
except Error as e:
    print(f"Error while connecting to MySQL: {e}")


def create_table_if_not_exists(table_name, create_query):
    try:
        cursor.execute(create_query)
        connection.commit()
        print(f"Table {table_name} is ready.")
    except Error as e:
        print(f"Failed to create table {table_name}: {e}")


def truncate_table(table_name):
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name};")
        connection.commit()
        print(f"Table {table_name} truncated successfully.")
    except Error as e:
        print(f"Failed to truncate table {table_name}: {e}")


def import_to_mysql(df, table_name, columns):
    try:
        # Truncate the table before inserting new data to avoid duplicates
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        connection.commit()
        print(f"Table {table_name} truncated successfully.")

        # Convert NaN to None (MySQL-compatible NULL)
        data = [tuple(None if pd.isna(value) else value for value in row) for row in df.to_numpy()]

        # Debugging: Print a sample of the data being inserted
        print(f"\nSample data for {table_name}:")
        print(data[:5])  # Print first 5 rows to verify data format

        # Prepare insert query
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # Insert data row by row
        cursor.executemany(insert_query, data)
        connection.commit()
        print(f"Data inserted successfully into {table_name} table")
    except Error as e:
        print(f"Failed to insert data into {table_name}: {e}")





def close_connection():
    if connection.is_connected():
        cursor.close()
        connection.close()
        print('MySQL connection closed')


# Table creation queries
products_table = '''
CREATE TABLE IF NOT EXISTS olist_products_cleaned_dataset (
    product_id VARCHAR(50),
    product_category VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_quantity INT,
    product_weight_gm INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);
'''

customers_table = '''
CREATE TABLE IF NOT EXISTS olist_customers_cleaned_dataset (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(50)
);
'''

orders_table = '''
CREATE TABLE IF NOT EXISTS olist_orders_cleaned_dataset (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp VARCHAR(50),
    order_approved_at VARCHAR(50),
    order_estimated_delivery_date VARCHAR(50),
    total_items INT,
    total_price FLOAT,
    total_freight_value FLOAT,
    total_payment_value FLOAT,
    payment_type VARCHAR(50),
    max_installments INT,
    avg_review_score INT
);
'''

sellers_table = '''
CREATE TABLE IF NOT EXISTS olist_sellers_cleaned_dataset (
    seller_id VARCHAR(50),
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(50)
);
'''


# Create tables if they do not exist
create_table_if_not_exists('olist_products_cleaned_dataset', products_table)
create_table_if_not_exists('olist_customers_cleaned_dataset', customers_table)
create_table_if_not_exists('olist_orders_cleaned_dataset', orders_table)
create_table_if_not_exists('olist_sellers_cleaned_dataset', sellers_table)


# Load the cleaned datasets
olist_products_cleaned_dataset = pd.read_csv(r'C:\Users\Mayank Pathak\Downloads\E - commerce pipeline cleaned data\olist_products_cleaned_dataset.csv')
olist_customers_cleaned_dataset = pd.read_csv(r'C:\Users\Mayank Pathak\Downloads\E - commerce pipeline cleaned data\olist_customers_cleaned_dataset.csv')
olist_orders_cleaned_dataset = pd.read_csv(r'C:\Users\Mayank Pathak\Downloads\E - commerce pipeline cleaned data\olist_orders_cleaned_dataset.csv')
olist_sellers_cleaned_dataset = pd.read_csv(r'C:\Users\Mayank Pathak\Downloads\E - commerce pipeline cleaned data\olist_sellers_cleaned_dataset.csv')


# Define table structures
products_columns = ['product_id', 'product_category', 'product_name_length', 'product_description_length',
                   'product_photos_quantity', 'product_weight_gm', 'product_length_cm', 'product_height_cm', 'product_width_cm']

customers_columns = ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']

orders_columns = ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at',
                  'order_estimated_delivery_date', 'total_items', 'total_price', 'total_freight_value',
                  'total_payment_value', 'payment_type', 'max_installments', 'avg_review_score']

sellers_columns = ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']


# Import data to MySQL tables
import_to_mysql(olist_products_cleaned_dataset, 'olist_products_cleaned_dataset', products_columns)
import_to_mysql(olist_customers_cleaned_dataset, 'olist_customers_cleaned_dataset', customers_columns)
import_to_mysql(olist_orders_cleaned_dataset, 'olist_orders_cleaned_dataset', orders_columns)
import_to_mysql(olist_sellers_cleaned_dataset, 'olist_sellers_cleaned_dataset', sellers_columns)


# Close the connection
close_connection()



data.to_csv(r"C:\Users\Mayank Pathak\Downloads\E - commerce pipeline cleaned data\Event_Data.csv", index = None)


data.info()