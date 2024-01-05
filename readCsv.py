import csv
from inspect import getcallargs
import pandas as pd

from operationalDataStore import odsClass
# This class reads the data values from a CSV file and uploads them to the operational data store
class readCsv:
    def __init__(self, csvFile):
        print("Parsing CSV...")
        self.df_csv = pd.read_csv(csvFile, encoding="ISO-8859-1")
        print("CSV Data")
        print(self.df_csv)

    def getCsvData(self):
        # Parse dates as the SQL date format
        # Adapted from GeeksForGeeks (Shubham__Ranjan, 2020)
        self.df_csv['date'] = pd.to_datetime(self.df_csv['date'])
        # End of adapted code

        # Rename columns to fit the operational data store - adapted from GeeksforGeeks (rituraj_jain, 2021) and Pandas Documentation (n.d)
        self.df_csv = self.df_csv.rename(columns={"sale": "SaleID", "employee":"EmployeeID", "date":"DateOfSale", "item":"ProductID", "quantity":"Quantity", "total":"SaleTotal"}, inplace=True)
        # End of adapted code

        # Upload to operational data store
        odsClass.df_FactSale = odsClass.df_FactSale.append(self.df_csv)
        return