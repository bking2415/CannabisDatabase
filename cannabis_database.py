# Cannabis Database
from bs4 import BeautifulSoup
import requests
import pandas as pd
from pandas import DataFrame
import base64
import numpy as np
# importing date class from datetime module
from datetime import date
# importing MySQL 
import mysql.connector
# importing functions from file
import cannabis_database_functions as cdf

# creating the date object of today's date
today = date.today()

# YY-mm-dd (SQL Format)
todays_date = today.strftime("%Y-%m-%d")

"""Weedmaps Data"""
# Web scraping of Weedmaps Data
# The Main Page
# Featured Brands on Main Page ("https://weedmaps.com/")
weedmaps_doc = cdf.load_data("https://weedmaps.com/")

# Featured Brands DataFrame
featured_brands_df = cdf.scrape_featured_brands_data(weedmaps_doc, todays_date)

print("Finished Scraping Featured Brands Data!")

# Find Products and Product Prices for Weedmaps
weedmaps_products_doc = cdf.load_data("https://weedmaps.com/products")

# Product Category Dictionary
product_category_lst = cdf.find_product_categories(weedmaps_products_doc)
# Popular Products DataFrame
# product_category_dict, popular_products_df = cdf.create_popular_products_data(product_category_lst, todays_date)

print("Finished Scraping Popular Products Data!")

# Connect to MySQL Server
# db = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     passwd="rea1dea1",
#     auth_plugin='mysql_native_password'
# )

# mycursor = db.cursor()

# mycursor.execute("CREATE DATABASE IF NOT EXISTS cannabisdatabase")
# Connect to MySQL Database
db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="rea1dea1",
    auth_plugin='mysql_native_password',
    database="cannabisdatabase"
)
print("Connected to Database!")
mycursor = db.cursor()

# create FeaturedBrands table
mycursor.execute("CREATE TABLE IF NOT EXISTS FeaturedBrands (brand VARCHAR(50), followers INT, brandRank SMALLINT UNSIGNED, dateExecuted DATE, executionId INT PRIMARY KEY AUTO_INCREMENT);")
# create PopularProducts table
mycursor.execute("CREATE TABLE IF NOT EXISTS PopularProducts (mainCategory VARCHAR(50), subCategory VARCHAR(50), brand VARCHAR(50), productName VARCHAR(100), averageStars FLOAT, reviews INT, price FLOAT, productRank SMALLINT UNSIGNED, source VARCHAR(50), dateExecuted DATE, executionId INT PRIMARY KEY AUTO_INCREMENT);")

# Insert Featured Brands DataFrame records one by one.
table_name = "FeaturedBrands"
columns = "(brand, followers, brandRank, dateExecuted)"
# cdf.add_dataframe_to_mysql(featured_brands_df, table_name, columns, db, mycursor)

# Insert Popular Products DataFrame records one by one.
table_name = "PopularProducts"
columns = "(mainCategory, subCategory, brand, productName, averageStars, reviews, price, productRank, source, dateExecuted)"
# cdf.add_dataframe_to_mysql(popular_products_df, table_name, columns, db, mycursor)
    
print("Finished Updating Weedmap.com Data in Database!")

"""Leafly Product Data"""
# Load Leafly Product Data
leafly_products_doc = cdf.load_data("https://www.leafly.com/products")
# Find Top 3 collection type
collection_type_rows = leafly_products_doc.find_all(["div"], attrs={"class":["jsx-425042678 flex items-end justify-between"]})
# Scrape Leafly Data
collection_products_df = cdf.scrape_all_collection_pages_product_data(collection_type_rows, todays_date)

print("Finished Scraping Leafly Products Data!")

# create LeaflyDailyProducts table
mycursor.execute("CREATE TABLE IF NOT EXISTS LeaflyDailyProducts (collectionType VARCHAR(50), brand VARCHAR(50), productName VARCHAR(100), price FLOAT, amount FLOAT, unit VARCHAR(25), pickUp BOOLEAN, distance FLOAT, distanceMetric VARCHAR(50), pageNumber INT, productRank SMALLINT UNSIGNED, source VARCHAR(50), dateExecuted DATE, executionId INT PRIMARY KEY AUTO_INCREMENT);")

# Insert LeaflyDailyProducts DataFrame records one by one.
table_name = "LeaflyDailyProducts"
columns = "(collectionType, brand, productName, price, amount, unit, pickUp, distance, distanceMetric, pageNumber, productRank, source, dateExecuted)"
cdf.add_dataframe_to_mysql(collection_products_df, table_name, columns, db, mycursor)

print("Finished Updating Leafly.com Data in Database!")