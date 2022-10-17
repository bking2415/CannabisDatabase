# Cannabis Database Funtions
from bs4 import BeautifulSoup
import requests
import pandas as pd
from pandas import DataFrame
import base64
import numpy as np

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
    # Create a dataframe for Featured Brands
    popular_products_df = pd.DataFrame.from_dict(popular_products_dict)
    # Add Source
    popular_products_df["source"] = "Weedmaps"
    # Add Current Date
    popular_products_df["date"] = current_date
    # Convert Python NaN values to NULL Values to translate to MySQL
    popular_products_df = popular_products_df.astype(object).where(pd.notnull(popular_products_df), None)
    return product_category_dict, popular_products_df