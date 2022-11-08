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
# importing Google Drive
import gspread
import gspread_dataframe as gd
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
product_category_dict, popular_products_df = cdf.create_popular_products_data(product_category_lst, todays_date)

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
cdf.add_dataframe_to_mysql(featured_brands_df, table_name, columns, db, mycursor)

# Insert Popular Products DataFrame records one by one.
table_name = "PopularProducts"
columns = "(mainCategory, subCategory, brand, productName, averageStars, reviews, price, productRank, source, dateExecuted)"
cdf.add_dataframe_to_mysql(popular_products_df, table_name, columns, db, mycursor)
    
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

"""Add Data to Google Sheets"""
# Connecting with `gspread` here
gc = gspread.service_account(filename='cannabisdatabase-c8145b23ad66.json')

# Add to FeatureBrands Google Sheets file
file_name = "CannabisDatabase"
sheet_name = "FeaturedBrands" 
weedmaps_brands_df = cdf.append_dataframe_to_google_sheets(gc, file_name, sheet_name, featured_brands_df)

# Add to PopularProducts Google Sheets file
sheet_name = "PopularProducts"
weedmaps_products_df = cdf.append_dataframe_to_google_sheets(gc, file_name, sheet_name, popular_products_df)

# Add to LeaflyDailyProducts Google Sheets file
sheet_name = "LeaflyDailyProducts"
leafly_products_df = cdf.append_dataframe_to_google_sheets(gc, file_name, sheet_name, collection_products_df)

print("Finished Adding Data to Google Sheets!")

"""Combining Product Data"""
sheet_lst = ["PopularProducts", "LeaflyDailyProducts"]
sub_columns = ["mainCategory", "brand", "productName","productRank", "source", "dateExecuted", "price"]
# Create csv file from existing google sheets data
products_df = cdf.create_csv_file_from_multiple_google_worksheets(gc, file_name, sheet_lst, sub_columns)

print("Finished Adding Product Data to CSV File!")

"""Predicting Next-Day Price of Products"""
# 1. Data-preprocessing

# # Get all rows from DataFrame where price is not NULL
# products_df = products_df[products_df['price'].notna()]
# # Drop duplicates
# products_df = products_df.drop_duplicates(subset=['productName', "dateExecuted"])
# # Remove rows in price equal string
# products_df = products_df[products_df['price'] != 'Pre-Rolls 2.5g 5-pack']
# # Change price column to float
# products_df["price"] = products_df["price"].astype('float')
# # Sort values by name then date
# products_df = products_df.sort_values(by=['productName', "dateExecuted"])
# # shifting by previous price
# products_df['prevPrice'] = products_df.groupby('productName')["price"].shift(1)

# # Calculate Price Difference
# products_df['priceDiff'] = (products_df['price'] - products_df['prevPrice'])

# # Change dateExecuted column to datetime
# products_df["dateExecuted"] = pd.to_datetime(products_df["dateExecuted"])
# # shifting by previous dates
# products_df['prevDate'] = products_df.groupby('productName')["dateExecuted"].shift(1)

# # Calculate Date Difference
# products_df['dateDiff'] = (products_df["dateExecuted"] - products_df['prevDate']).dt.days

# # Get all rows from DataFrame where prevPrice is not NULL
# products_df = products_df[products_df['prevPrice'].notna()]
# # Get all rows from DataFrame where prevDate is not NULL
# products_df = products_df[products_df['prevDate'].notna()]

# # Get day, month and year from datetime column
# # products_df['day'] = pd.DatetimeIndex(products_df["dateExecuted"]).day
# # products_df['month'] = pd.DatetimeIndex(products_df["dateExecuted"]).month
# # products_df['year'] = pd.DatetimeIndex(products_df["dateExecuted"]).year
# # Change mainCategory column to category
# products_df["mainCategory"] = products_df["mainCategory"].astype('category')

# print_columns = ['productName', "dateExecuted", 'prevDate', 'dateDiff', "price", 'prevPrice', 'priceDiff']
# print(products_df[print_columns].head(10))
# print(products_df.shape)
# print(products_df.dtypes) 
