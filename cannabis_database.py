# Cannabis Database (Weedmaps)
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

# # Connect to MySQL Server
# db = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     passwd="rea1dea1",
#     auth_plugin='mysql_native_password'
# )

# mycursor = db.cursor()

# mycursor.execute("CREATE DATABASE IF NOT EXISTS cannabisdatabase")
# # Connect to MySQL database
# db = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     passwd="rea1dea1",
#     auth_plugin='mysql_native_password',
#     database="cannabisdatabase"
# )

# mycursor = db.cursor()

# # create FeaturedBrands table
# mycursor.execute("CREATE TABLE IF NOT EXISTS FeaturedBrands (brand VARCHAR(50), followers INT, brandRank SMALLINT UNSIGNED, dateExecuted DATE, executionId INT PRIMARY KEY AUTO_INCREMENT);")
# # create PopularProducts table
# mycursor.execute("CREATE TABLE IF NOT EXISTS PopularProducts (mainCategory VARCHAR(50), subCategory VARCHAR(50), brand VARCHAR(50), productName VARCHAR(100), averageStars FLOAT, reviews INT, price FLOAT, productRank SMALLINT UNSIGNED, source VARCHAR(50), dateExecuted DATE, executionId INT PRIMARY KEY AUTO_INCREMENT);")

# # making dataframe 
# f_df = pd.read_csv("weedmaps_featured_brands.csv") 

# # Insert Featured Brands DataFrame records one by one.
# for i, row in f_df.iterrows():
#     sql = "INSERT INTO FeaturedBrands (brand, followers, brandRank, dateExecuted) VALUES (" + "%s,"*(len(row)-1) + "%s)"
#     mycursor.execute(sql, tuple(row))

#     # the connection is not autocommitted by default, so we must commit to save our changes
#     db.commit()
    

# p_df = pd.read_csv("weedmaps_popular_products.csv") 
# p_df = p_df.astype(object).where(pd.notnull(p_df), None)

# # Insert Popular Products DataFrame records one by one.
# for i, row in p_df.iterrows():
#     sql = "INSERT INTO PopularProducts (mainCategory, subCategory, brand, productName, averageStars, reviews, price, productRank, source, dateExecuted) VALUES (" + "%s,"*(len(row)-1) + "%s)"
#     mycursor.execute(sql, tuple(row))

#     # the connection is not autocommitted by default, so we must commit to save our changes
#     db.commit()

# creating the date object of today's date
today = date.today()

# YY-mm-dd (SQL Format)
todays_date = today.strftime("%Y-%m-%d")


# Function to load data from webpage
def load_data(webpage_link):
    url = webpage_link
    webpage = requests.get(url).text
    doc = BeautifulSoup(webpage, "html.parser")
    return doc

# Web scraping of Weedmaps Data
# The Main Page
# Featured Brands on Main Page ("https://weedmaps.com/")
weedmaps_doc = load_data("https://weedmaps.com/")

# Function to Find Featured Brands
def scrape_featured_brands_data(data_doc, current_date):
    # Find featured brands row
    featured_brands_row = data_doc.find(["div"], attrs={"data-testid":"featured-brands-row"})
    # Identify feature brand contents
    featured_brands_content = featured_brands_row.contents[1].contents[0].contents[0]
    featured_brands_dict = {'brand': [], 'followers': []}
    # Loop through featured brands
    for featured_brand in featured_brands_content:
        brand, num_of_followers =  featured_brand.contents[0].contents[0].contents[1]
        # Add brand name to list (Remove text from div)
        featured_brands_dict['brand'].append(brand.text)
        # Add brand followers to list (Remove string from span)
        num_of_followers = ''.join([i for i in num_of_followers.string if i.isdigit()])
        featured_brands_dict['followers'].append(int(num_of_followers))

    # Create a dataframe for Featured Brands
    featured_brands_df = pd.DataFrame.from_dict(featured_brands_dict)
    # Add Rank based on Row
    featured_brands_df["rank"] = featured_brands_df.reset_index().index + 1
    # Add Current Date
    featured_brands_df["date"] = current_date
    return featured_brands_df

# Function featured brand df
featured_brands_df = scrape_featured_brands_data(weedmaps_doc, todays_date)
    
# featured_brands_row = weedmaps_doc.find(["div"], attrs={"data-testid":"featured-brands-row"})

# # Identify feature brand contents
# featured_brands_content = featured_brands_row.contents[1].contents[0].contents[0]

# featured_brands_dict = {'brand': [], 'followers': []}
# # Loop through featured brands
# for featured_brand in featured_brands_content:
#     brand, num_of_followers =  featured_brand.contents[0].contents[0].contents[1]
#     # Add brand name to list (Remove text from div)
#     featured_brands_dict['brand'].append(brand.text)
#     # Add brand followers to list (Remove string from span)
#     num_of_followers = ''.join([i for i in num_of_followers.string if i.isdigit()])
#     featured_brands_dict['followers'].append(int(num_of_followers))

# # Create a dataframe for Featured Brands
# featured_brands_df = pd.DataFrame.from_dict(featured_brands_dict)
# # Add Rank based on Row
# featured_brands_df["rank"] = featured_brands_df.reset_index().index + 1
# # Add Current Date
# featured_brands_df["date"] = todays_date


# print(featured_brands_df.equals(func_featured_brands_df))

print("Finished Scraping Featured Brands Data!")


# Find Products and Product Prices for Weedmaps
weedmaps_products_doc = load_data("https://weedmaps.com/products")

# Function to Find Product Categories
def find_product_categories(data_doc):
    # Find product category box
    product_category_content = data_doc.find(["div"], attrs={"data-testid":"L1-grid"})
    # Define different type of product categories
    product_category_lst = []
    # Loop through product category
    for product_category in product_category_content:
        item = product_category.text
        product_category_lst.append(item.lower().replace(' ', '-'))
    return product_category_lst

# Product Category Dictionary
product_category_lst = find_product_categories(weedmaps_products_doc)

# Function to Scrape Popular Brands Data
def scrape_popular_products_data(data_doc, popular_products_dict, main_cat, sub_cat):
    # Find popular items and details for each sub category
    popular_items_row = data_doc.find(["div"], class_="styles__TrendingProductsGrid-sc-1ewyv2z-8 icDLUQ")
    # print(len(popular_items_row))
    if popular_items_row is None:
        return popular_products_dict
    # Loop through popular products
    for popular_products in popular_items_row:
        # Add Main Category
        popular_products_dict['main_category'].append(main_cat)
        # Add Sub Category
        popular_products_dict['sub_category'].append(sub_cat)
        for key, product in enumerate(popular_products.contents[1].contents):
            # Add product brand
            if key == 0:
                brand = product.text
                if brand:
                    popular_products_dict['brand'].append(brand)
                else:
                    popular_products_dict['brand'].append(None)
            # Add product name
            elif key == 1:
                product_name = product.text
                popular_products_dict['product_name'].append(product_name)
            elif key == 2:
                if '$' in product.text:
                    average_stars = None
                    popular_products_dict['average_stars'].append(average_stars)
                    num_of_reviews = None
                    popular_products_dict['number_of_reviews'].append(num_of_reviews)
                    # Add dollar amount to price
                    price = ''.join([i for i in product.text if i.isdigit() or i == '.'])
                    price = '.'.join(price.split(".")[:2])
                    popular_products_dict['price'].append(float(price))
                else:
                    # Add average star amount
                    average_stars = product.text.split(" ")[0].strip()
                    popular_products_dict['average_stars'].append(float(average_stars))
                    # Add number of reviews
                    num_of_reviews = product.text.split("(")[1].replace(")", "")
                    popular_products_dict['number_of_reviews'].append(int(num_of_reviews))
            else:
                # Add dollar amount to price
                price = ''.join([i for i in product.text if i.isdigit() or i == '.'])
                price = '.'.join(price.split(".")[:2])
                popular_products_dict['price'].append(float(price))
    # Add rank of products
    popular_products_dict['rank'] += list(range(1,len(popular_items_row)+1))  
    return popular_products_dict

    

# Function to Create Popular Products DataFrame
def create_popular_products_data(list_products, current_date):
    # Product Category and Subcategory Dictionary
    product_category_dict = {}
    # Popular products Dictionary
    popular_products_dict = {'main_category': [], 'sub_category': [],'brand': [], 'product_name': [], 'average_stars': [], 'number_of_reviews': [], 'price': [], 'rank': []}
    # Identify the Subcategories from Product Categories
    for category in list_products[:-1]:
        product_category_dict[category] = []
        # Identify popular items by category
        weedmaps_products_by_category_doc = load_data("https://weedmaps.com/products/" + category)
        # Sub Category
        sub_category = category
        # Scrape Popular products from main category
        popular_products_dict = scrape_popular_products_data(weedmaps_products_by_category_doc, popular_products_dict, category, sub_category)
        # Find the sub_categories for each main category
        product_by_category_content = weedmaps_products_by_category_doc.find(["div"], class_="src__Box-sc-1sbtrzs-0 src__Flex-sc-1sbtrzs-1 knowledge-panel-toggles__ToggleGroup-sc-1l6kilu-0 iSbMbQ dtEVmG hBAxFN")
        if product_by_category_content is None:
            break
        # Loop through sub-items in category
        for product_sub_category in product_by_category_content:
            # Sub Category
            sub_item = product_sub_category.string
            sub_category = sub_item.lower().replace('&', '').replace('  ', ' ').replace(' ', '-')
            product_category_dict[category].append(sub_category)
            if category != sub_category:
                # Identify popular items by category
                weedmaps_products_by_category_doc = load_data("https://weedmaps.com/products/" + category + "/" + sub_category)
                # Scrape Popular products from main category
                popular_products_dict = scrape_popular_products_data(weedmaps_products_by_category_doc, popular_products_dict, category, sub_category)
    # Create a dataframe for Featured Brands
    popular_products_df = pd.DataFrame.from_dict(popular_products_dict)
    # Add Source
    popular_products_df["source"] = "Weedmaps"
    # Add Current Date
    popular_products_df["date"] = current_date
    # Convert Python NaN values to NULL Values to translate to MySQL
    popular_products_df = popular_products_df.astype(object).where(pd.notnull(popular_products_df), None)
    return product_category_dict, popular_products_df

product_category_dict, popular_products_df = create_popular_products_data(product_category_lst, todays_date)


# product_category_content = weedmaps_products_doc.find(["div"], attrs={"data-testid":"L1-grid"})

# # Define different type of product categories
# product_category_lst = []
# # Loop through product category
# for product_category in product_category_content:
#     item = product_category.text
#     product_category_lst.append(item.lower().replace(' ', '-'))
    
# # print(product_category_lst == func_product_category_lst)

# product_category_dict = {}

# # Identify the Subcategories from Product Categories
# for category in product_category_lst[:-1]:
#     product_category_dict[category] = []
#     # Identify popular items by category
#     weedmaps_products_by_category_doc = load_data("https://weedmaps.com/products/" + category)
#     # Find the sub_categories for each main category
#     product_by_category_content = weedmaps_products_by_category_doc.find(["div"], class_="src__Box-sc-1sbtrzs-0 src__Flex-sc-1sbtrzs-1 knowledge-panel-toggles__ToggleGroup-sc-1l6kilu-0 iSbMbQ dtEVmG hBAxFN")
# #<div class="src__Box-sc-1sbtrzs-0 src__Flex-sc-1sbtrzs-1 knowledge-panel-toggles__ToggleGroup-sc-1l6kilu-0 iSbMbQ dtEVmG hBAxFN"><a class="text__Text-sc-fif1uk-0 knowledge-panel-toggles__ToggleButton-sc-1l6kilu-1 hniXLZ lcSTWB isActive" href="/products/flower"><span class="text__Text-sc-fif1uk-0 hniXLZ">Flower</span></a><a class="text__Text-sc-fif1uk-0 knowledge-panel-toggles__ToggleButton-sc-1l6kilu-1 hniXLZ lcSTWB" href="/products/flower/infused-flower"><span class="text__Text-sc-fif1uk-0 hniXLZ">Infused Flower</span></a><a class="text__Text-sc-fif1uk-0 knowledge-panel-toggles__ToggleButton-sc-1l6kilu-1 hniXLZ lcSTWB" href="/products/flower/pre-roll"><span class="text__Text-sc-fif1uk-0 hniXLZ">Pre Roll</span></a><a class="text__Text-sc-fif1uk-0 knowledge-panel-toggles__ToggleButton-sc-1l6kilu-1 hniXLZ lcSTWB" href="/products/flower/shake"><span class="text__Text-sc-fif1uk-0 hniXLZ">Shake</span></a></div>
#     # Loop through items in category
#     for product_sub_category in product_by_category_content:
#         sub_item = product_sub_category.string
#         product_category_dict[category].append(sub_item.lower().replace('&', '').replace('  ', ' ').replace(' ', '-'))
        
# # print(product_category_dict==func_product_category_dict)

# popular_products_dict = {'main_category': [], 'sub_category': [],'brand': [], 'product_name': [], 'average_stars': [], 'number_of_reviews': [], 'price': [], 'rank': []}

# for main_category in product_category_dict.keys():
#     for sub_category in product_category_dict[main_category]:
#         # print(sub_category)
#         if main_category == sub_category:
#             # Identify popular items by category
#             weedmaps_products_by_category_doc = load_data("https://weedmaps.com/products/" + main_category)
#             # Main Category
#             main_cat = main_category
#             # Sub Category
#             sub_cat = main_category
#         else:
#             # Identify popular items by category
#             weedmaps_products_by_category_doc = load_data("https://weedmaps.com/products/" + main_category + "/" + sub_category)
#             # Main Category
#             main_cat = main_category
#             # Sub Category
#             sub_cat = sub_category
#         # print(popular_products_dict)
#         # Find popular items and details for each sub category
#         popular_items_row = weedmaps_products_by_category_doc.find(["div"], class_="styles__TrendingProductsGrid-sc-1ewyv2z-8 icDLUQ")
#         # print(len(popular_items_row))
#         if popular_items_row is None:
#             break
        
#         # popular_products_dict = {'main_category': [], 'sub_category': [],'brand': [], 'product_name': [], 'average_stars': [], 'number_of_reviews': [], 'price': []}
#         # Loop through popular products
#         for popular_products in popular_items_row:
#             # Add Main Category
#             popular_products_dict['main_category'].append(main_cat)
#             # Add Sub Category
#             popular_products_dict['sub_category'].append(sub_cat)
#             for key, product in enumerate(popular_products.contents[1].contents):
#                 # Add product brand
#                 if key == 0:
#                     brand = product.text
#                     if brand:
#                         popular_products_dict['brand'].append(brand)
#                     else:
#                         popular_products_dict['brand'].append(None)
#                 # Add product name
#                 elif key == 1:
#                     product_name = product.text
#                     popular_products_dict['product_name'].append(product_name)
#                 elif key == 2:
#                     if '$' in product.text:
#                         average_stars = None
#                         popular_products_dict['average_stars'].append(average_stars)
#                         num_of_reviews = None
#                         popular_products_dict['number_of_reviews'].append(num_of_reviews)
#                         # Add dollar amount to price
#                         price = ''.join([i for i in product.text if i.isdigit() or i == '.'])
#                         price = '.'.join(price.split(".")[:2])
#                         popular_products_dict['price'].append(float(price))
#                     else:
#                         # Add average star amount
#                         average_stars = product.text.split(" ")[0].strip()
#                         popular_products_dict['average_stars'].append(float(average_stars))
#                         # Add number of reviews
#                         num_of_reviews = product.text.split("(")[1].replace(")", "")
#                         popular_products_dict['number_of_reviews'].append(int(num_of_reviews))
#                 else:
#                     # Add dollar amount to price
#                     price = ''.join([i for i in product.text if i.isdigit() or i == '.'])
#                     price = '.'.join(price.split(".")[:2])
#                     popular_products_dict['price'].append(float(price))
#         # Add rank of products
#         popular_products_dict['rank'] += list(range(1,len(popular_items_row)+1))  

# # print(popular_products_dict)
# # for key in popular_products_dict.keys():
# #     print(len(popular_products_dict[key]))
# #     print()
# # Create a dataframe for Featured Brands
# popular_products_df = pd.DataFrame.from_dict(popular_products_dict)
# # Add Source
# popular_products_df["source"] = "Weedmaps"
# # Add Current Date
# popular_products_df["date"] = todays_date
# # Convert Python NaN values to NULL Values to translate to MySQL
# popular_products_df = popular_products_df.astype(object).where(pd.notnull(popular_products_df), None)


print("Finished Scraping Popular Products Data!")

print(len(popular_products_df))

# Create csv files for DataFrames
# output_path="weedmaps_featured_brands.csv"
# featured_brands_df.to_csv(output_path, mode='a')

# output_path="weedmaps_popular_products1.csv"
# popular_products_df.to_csv(output_path, mode='w')

# output_path="weedmaps_popular_products2.csv"
# func_popular_products_df.to_csv(output_path, mode='w')

# # Connect to MySQL Server
# # db = mysql.connector.connect(
# #     host="localhost",
# #     user="root",
# #     passwd="rea1dea1",
# #     auth_plugin='mysql_native_password'
# # )

# # mycursor = db.cursor()

# # mycursor.execute("CREATE DATABASE IF NOT EXISTS cannabisdatabase")
# # Connect to MySQL Database
# db = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     passwd="rea1dea1",
#     auth_plugin='mysql_native_password',
#     database="cannabisdatabase"
# )
# print("Connected to Database!")
# mycursor = db.cursor()

# # create FeaturedBrands table
# mycursor.execute("CREATE TABLE IF NOT EXISTS FeaturedBrands (brand VARCHAR(50), followers INT, brandRank SMALLINT UNSIGNED, dateExecuted DATE, executionId INT PRIMARY KEY AUTO_INCREMENT);")
# # create PopularProducts table
# mycursor.execute("CREATE TABLE IF NOT EXISTS PopularProducts (mainCategory VARCHAR(50), subCategory VARCHAR(50), brand VARCHAR(50), productName VARCHAR(100), averageStars FLOAT, reviews INT, price FLOAT, productRank SMALLINT UNSIGNED, source VARCHAR(50), dateExecuted DATE, executionId INT PRIMARY KEY AUTO_INCREMENT);")


# # Insert Featured Brands DataFrame records one by one.
# for i, row in featured_brands_df.iterrows():
#     sql = "INSERT INTO FeaturedBrands (brand, followers, brandRank, dateExecuted) VALUES (" + "%s,"*(len(row)-1) + "%s)"
#     mycursor.execute(sql, tuple(row))

#     # the connection is not autocommitted by default, so we must commit to save our changes
#     db.commit()


# # Insert Popular Products DataFrame records one by one.
# for i, row in popular_products_df.iterrows():
#     sql = "INSERT INTO PopularProducts (mainCategory, subCategory, brand, productName, averageStars, reviews, price, productRank, source, dateExecuted) VALUES (" + "%s,"*(len(row)-1) + "%s)"
#     mycursor.execute(sql, tuple(row))

#     # the connection is not autocommitted by default, so we must commit to save our changes
#     db.commit()
    
# print("Finished Updating Database!")