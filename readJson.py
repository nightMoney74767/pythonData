import json
from math import prod
import pandas as pd

from operationalDataStore import odsClass
# This class reads the data values in a JSON file and uploads them to the operational data store
class readJson:
    def __init__(self, jsonFile):
        print("Fetching JSON file...")
        try:
            self.file = jsonFile.encode("ISO-8859-1")
            with open(jsonFile) as f:
                self.getJsonData = json.load(f)
        except:
            print("Failed to fetch JSON file")
            return

    def getJsonSales(self):
        df_sales = pd.json_normalize(data = self.getJsonData['Sale'])
        # Rename columns to match the operational data store as much as possible
        df_sales = df_sales.rename(columns={"SaleTax": "SalesTax", "Product": "ProductID", "SubTotal": "SaleAmount"})
        
        # Parse dates as the SQL date format
        # Adapted from GeeksForGeeks (Shubham__Ranjan, 2020)
        df_sales['DateOfSale'] = pd.to_datetime(df_sales['DateOfSale'])
        # End of adapted code

        # Rename sale IDs to avoid conflicts with sale IDs in the CSV file
        # Adapted from StackOverflow (Roman Pekar, 2013)
        df_sales['SaleID'] = df_sales['SaleID'].astype(str) + "-JSON"
        # End of adapted code

        print(df_sales)
        
        # Add to operational data store
        odsClass.df_FactSale = odsClass.df_FactSale.append(df_sales)

    def getJsonProducts(self):
        df_product = pd.json_normalize(data = self.getJsonData['Product'])
        # Get average price for the operational data store. 
        # As there are minimum and maximum prices in the JSON, this is determined to be the best course of action
        df_product['MaxPrice'] = pd.DataFrame(df_product['prices.amountMax'])
        df_product['MinPrice'] = pd.DataFrame(df_product['prices.amountMin'])
        # Adapted from StackOverflow (Stefan, 2016)
        df_product['ProductPrice'] = df_product[['MaxPrice', 'MinPrice']].mean(axis = 1)

        # Rename columns to match the operational data store as much as possible
        df_product = df_product.rename(columns={"id": "ProductID", "manufacturer": "SupplierID", "name": "ProductDescription"})

        print(df_product)

        # Add to operational data store
        odsClass.df_DimProduct = odsClass.df_DimProduct.append(df_product)