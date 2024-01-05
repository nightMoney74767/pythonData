# This is the main class which executes all of the individual Python classes
import csv
from operationalDataStore import odsClass
from readSql import readSql
from readCsv import readCsv
from readJson import readJson
from exportData import exportData

class mainClass:
    # Database connection string for parsing SQL
    sqlParseString = (
            'Driver={SQL Server};'
            'Server=REDACTED;'
            'Database=REDACTED;'
            'UID=REDACTED;'
            'PWD=REDACTED;'
    )

    # Database connection string for exporting data
    exportString = (
        'Driver={SQL Server};'
        'Server=REDACTED;'
        'Database=REDACTED;'
        'UID=REDACTED;'
        'PWD=REDACTED;'
    )

    # CSV file
    csvFile = "SalesCSV.csv"

    # JSON file
    jsonFile = "SalesJSON.json"

    def __init__(self):
        print("Python code started")

        # SQL
        parseSql = readSql(self.sqlParseString)
        parseSql.sqlGetSale()
        print("SQL parsing complete")

        # CSV
        parseCsv = readCsv(self.csvFile)
        parseCsv.getCsvData()
        print("CSV parsing complete")

        # JSON
        parseJson = readJson(self.jsonFile)
        parseJson.getJsonSales()
        parseJson.getJsonProducts()
        print("JSON parsing complete")

        # Remove columns that do not fit the operational data store (added when parsing data)
        # Adapted from StackOverflow (LondonRob, 2013)
        odsClass.df_DimEmployee.drop(['JobTitleID'], axis=1, inplace=True)
        odsClass.df_DimJob.drop('PayFrequencyID', axis=1, inplace=True)
        odsClass.df_DimProduct.drop(['primaryCategories', 'brand'], axis=1, inplace=True)
        odsClass.df_DimProduct.drop(['prices.amountMin', 'prices.currency', 'MaxPrice',
       'MinPrice', 'prices.amountMax'], axis=1, inplace=True)
        odsClass.df_FactSale.drop(['TaxRate', "Sales", "Delivery", "Customer"], axis=1, inplace=True)
        # End of adapted code

        # Get columns
        print("Columns for FactSale: ", odsClass.df_FactSale.columns)
        print("Columns for DimCustomer: ", odsClass.df_DimCustomer.columns)
        print("Columns for DimEmployee: ", odsClass.df_DimEmployee.columns)
        print("Columns for DimEmployeeStatus: ", odsClass.df_DimEmployeeStatus.columns)
        print("Columns for DimJob: ", odsClass.df_DimJob.columns)
        print("Columns for DimStore: ", odsClass.df_DimStore.columns)
        print("Columns for DimProduct: ", odsClass.df_DimProduct.columns)
        print("Columns for DimSupplier: ", odsClass.df_DimSupplier.columns)

        # Export data
        export = exportData(self.exportString)
        export.buildExportTables()
        export.exportOperationalDataStore()
        print("Export complete")

        print("Python code finished executing")
mainClass = mainClass()