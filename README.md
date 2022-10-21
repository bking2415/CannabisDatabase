# CannabisDatabase
Automated Data Warehouse to store Cannabis Product Data. This projects scrapes popular cannabis/marijuana webpages and collects the data of brands and products to store within a database and create automated data visualizations.

**Data Sources:**
- [Weedmaps](https://weedmaps.com/)
- [Leafly](https://www.leafly.com/)

## Instructions
1. Make sure all the proper libraries are imported (i.e., _pandas, numpy_) and packages are installed.
   - **Command Line:** `pip3 install -r requirements.txt`
2. Run `cannabis_database.py` to run the main code.
   - **Command Line:** `python3 cannabis_database.py`

## Project Structure 

- `cannabis_database.py` -- The implementation of webscraping source's urls and storing the data into local MySQL Workbench and Google Sheets.
- `cannabis_database_functions.py` -- The function file corresponding to the main python code.

## Data Visualization
- **Mode Analytics**: [Cannabis Analysis](https://app.mode.com/brandon_mysql/reports/685d72506109)
- **Tableau Public**: [Featured Brands Product Analysis](https://public.tableau.com/views/FeaturedBrandsProductAnalysis/NumberofProductsbyCategory?:language=en-US&:display_count=n&:origin=viz_share_link)
