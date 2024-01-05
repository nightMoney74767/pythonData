from numpy import product
import pandas as pd
from pandas.core.accessor import DirNamesMixin
from pandas.io.formats.format import Datetime64Formatter
from pandas.tseries.offsets import CustomBusinessHour
import pyodbc

# This class sets up the fact and dimension tables
# Populate class with tables from schema
tables = [
    'DimCustomer',
    'DimEmployeeStatus',
    'DimEmployee',
    'DimJob',
    'DimStore',
    'DimProduct',
    'DimSupplier',
    'FactSale',
]

class odsClass:
    # Create data-frames for schema tables
    # Each data-frame is defined in this order so that data can be linked in an SQL database (where all parsed data is exported)
    # Dimension tables
    df_DimCustomer = pd.DataFrame(columns=['CustomerID', 'CustomerEmail', 'FirstName', 'SecondName', 'CustomerType', 'City', 'StateProvince', 'Country', 'PostalCode'])
    df_DimEmployeeStatus = pd.DataFrame(columns=['StatusID', 'StatusDescription'])
    df_DimEmployee = pd.DataFrame(columns=['EmployeeID', 'FirstName', 'LastName', 'BirthDate', 'HireDate', 'EndDate', 'EmailAddress', 'Phone', 'EmergencyContactPhone', 'StatusID', 'StoreID'])
    df_DimJob = pd.DataFrame(columns=['JobTitleID', 'JobTitle', 'JobDescription', 'Salaried', 'PayRate', 'VacationHours', 'SickLeaveAllowance', 'SalesPersonFlag'])
    df_DimStore = pd.DataFrame(columns=['StoreID', 'StoreAddress', 'StoreCity', 'StoreStateProvince', 'StoreCountry', 'StorePostCode', 'StorePhone'])
    df_DimProduct = pd.DataFrame(columns=['ProductID', 'ProductDescription', 'CategoryID', 'SupplierPrice', 'ProductPrice', 'SafetyStockLevel', 'ReorderPoint', 'SupplierID'])
    df_DimSupplier = pd.DataFrame(columns=['SupplierID', 'SupplierAddress', 'SupplierCity', 'SupplierStateProvince', 'SupplierCountry', 'SupplierPostCode', 'SupplierPhone'])
    
    # Fact table
    df_FactSale = pd.DataFrame(columns=['SaleID', 'ProductID', 'CustomerID', 'DateOfSale', 'Quantity', 'SaleAmount', 'SalesTax', 'SaleTotal', 'StoreID', 'DateShipped', 'ShippingType', 'EmployeeID', 'SupplierID', 'JobTitleID'])