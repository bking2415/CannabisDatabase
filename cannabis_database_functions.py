# Cannabis Database Funtions
from bs4 import BeautifulSoup
import requests
import pandas as pd
from pandas import DataFrame
import base64
import numpy as np
# importing Google Drive
import gspread
import gspread_dataframe as gd

# Function to load data from webpage
def load_data(webpage_link):
    url = webpage_link
    webpage = requests.get(url).text
    doc = BeautifulSoup(webpage, "html.parser")
    return doc

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
    # Create a dataframe for Popular Products
    popular_products_df = pd.DataFrame.from_dict(popular_products_dict)
    # Add Source
    popular_products_df["source"] = "Weedmaps"
    # Add Current Date
    popular_products_df["date"] = current_date
    # Convert Python NaN values to NULL Values to translate to MySQL
    popular_products_df = popular_products_df.astype(object).where(pd.notnull(popular_products_df), None)
    return product_category_dict, popular_products_df

# Function to Scrape Individual Collection Page from Leafly.com
def scrape_collection_page_product_data(data_doc, collections_dict, collections_type, page_number):
    # Find products and details
    products_collection = data_doc.find(class_="mb-section row")
    # Filter only products in the container
    products_text = products_collection.find_all(["article"], attrs={"class":["ct-product-card-v2"]})
    for product in products_text:
        # Add collection type to dictionary
        collections_dict['collection'].append(collections_type)
        # Add collection type to dictionary
        collections_dict['page_number'].append(page_number)
        # Product Name and Brand
        product_details = product.contents[0].contents[0].contents[1].text
        product_name = product_details.split("by")[0]
        # Add product_name to dictionary
        collections_dict['product_name'].append(product_name)
        brand = product_details.split("by")[1].split(" ")[1]
        brand = ''.join([i for i in brand if i.isalpha() or i == " "]).lstrip().rstrip()
        # Add brand to dictionary
        collections_dict['brand'].append(brand)
        # Product detail check
        if len(product.contents[0].contents[1].contents) == 3 and product.contents[0].contents[1].contents != []:
            # Product amount and unit | price | pick-up and distance
            product_dimensions, product_price, pick_up_credentials = product.contents[0].contents[1].contents
            # Product amount
            amount = float(product_dimensions.text.split(" ")[0])
            # Add amount to dictionary
            collections_dict['amount'].append(amount)
            # Product unit
            unit = product_dimensions.text.split(" ")[1]
            # Add unit to dictionary
            collections_dict['unit'].append(unit)
            # Product price
            price = float(''.join([i for i in product_price.text if i.isdigit() or i == '.']))
            # Add price to dictionary
            collections_dict['price'].append(price)
            # Product Pickup Credentials
            if pick_up_credentials.text.split(" ")[0].lower() == "pickup":
                # Product Pick-Up
                pick_up = 1
                # Product Distance
                distance = float(pick_up_credentials.text.split(" ")[1])
                # Product Distance Metric
                distance_metric = pick_up_credentials.text.split(" ")[2]
            else:
                # Product Pick-Up
                pick_up = 0
                # Product Distance
                distance = None
                # Product Distance Metric
                distance_metric = None
        else:
            # Add amount to dictionary
            collections_dict['amount'].append(None)
            # Add unit to dictionary
            collections_dict['unit'].append(None)
            # Add price to dictionary
            collections_dict['price'].append(None)
            # Product Pick-Up
            pick_up = 0
            # Product Distance
            distance = None
            # Product Distance Metric
            distance_metric = None
        # Add pick_up to dictionary
        collections_dict['pick_up'].append(pick_up)
        # Add distance to dictionary
        collections_dict['distance'].append(distance)
        # Add distance metric to dictionary
        collections_dict['distance_metric'].append(distance_metric)
    print(f"Done Scraping Page {page_number}!")
    return collections_dict

# Function to scrape all pages for each collection
def scrape_all_collection_pages_product_data(collection_row, current_date):
    # Collections Products Dictionary
    collection_products_dict = {'collection': [],'brand': [], 'product_name': [], 'price': [], 'amount': [], 'unit': [], 'pick_up': [], 'distance': [], 'distance_metric': [], 'page_number': [], 'rank': []}
    # Identify collection contents
    for row in collection_row:
        # Collection Type
        collection_type = row.text.split(" ")[0].lower()
        # Find All Products and Product Prices for Collection (Leafly)
        collection_url = f"https://www.leafly.com/products/collections/{collection_type}"
        leafly_collections_products_doc = load_data(collection_url)
        # Page text
        page_text = leafly_collections_products_doc.find(class_="pagination mb-section")
        # Number of Pages
        pages = int(page_text.contents[-2].text)
        for page in range(1, pages+1):
            # Get url for page number
            page_url = f"https://www.leafly.com/products/collections/{collection_type}?page={page}"
            collections_products_page_doc = load_data(page_url)
            # Dynamically get pages from 
            collection_products_dict = scrape_collection_page_product_data(collections_products_page_doc, collection_products_dict, collection_type, page)
        # Number of products per collections
        num_of_products = collection_products_dict['collection'].count(collection_type)
        # Add rank of products
        collection_products_dict['rank'] += list(range(1,num_of_products+1))
        print(f"Done Scraping Collection {collection_type}!")
    # Create a dataframe for Collections Products
    collection_products_df = pd.DataFrame.from_dict(collection_products_dict)
    # Add Source
    collection_products_df["source"] = "Leafly"
    # Add Current Date
    collection_products_df["date"] = current_date
    # Convert Python NaN values to NULL Values to translate to MySQL
    collection_products_df = collection_products_df.astype(object).where(pd.notnull(collection_products_df), None) 
    return collection_products_df

"""MySQL Database Functions"""
# Function to add DataFrame to MySQL Database
def add_dataframe_to_mysql(dataframe, table_name, table_columns, database_connection, cursor):
    # Insert Featured Brands DataFrame records one by one.
    for i, row in dataframe.iterrows():
        sql = f"INSERT INTO {table_name} {table_columns} VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row))

        # the connection is not autocommitted by default, so we must commit to save our changes
        database_connection.commit()
        
"""Google Sheets Functions"""
def append_dataframe_to_google_sheets(google_connector, file_name, sheet_name, df):
    ws = google_connector.open(file_name).worksheet(sheet_name)
    existing = gd.get_as_dataframe(ws)
    existing = existing.dropna(how='all') # Drop all NULL Rows
    existing = existing.dropna(axis=1, how='all') # Drop all NULL Columns
    df.columns = existing.columns # Change column names
    updated = pd.concat([existing, df], axis=0, ignore_index=True).reset_index(drop=True)
    gd.set_with_dataframe(ws, updated) # Add to Google Sheets
    return updated 

def create_csv_file_from_multiple_google_worksheets(google_connector, file_name, worksheet_list, columns):
    # create an Empty DataFrame object
    products_df= pd.DataFrame()
    for sheet_name in worksheet_list:
        ws = google_connector.open(file_name).worksheet(sheet_name)
        existing = gd.get_as_dataframe(ws)
        existing = existing.dropna(how='all') # Drop all NULL Rows
        existing = existing.dropna(axis=1, how='all') # Drop all NULL Columns
        # Subset of Products Data
        sub_products_df = existing[columns]
        products_df = pd.concat([products_df, sub_products_df], axis=0, ignore_index=True).reset_index(drop=True)
    # Add to csv file
    products_df.to_csv("cannabis_products.csv",index=False) 
    return products_df