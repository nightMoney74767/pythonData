import turtle
from pandas.core.accessor import DirNamesMixin
from operationalDataStore import odsClass
import pandas as pd
import pyodbc
class readSql:
    # This class extracts data from SQL into the ODS, using the class operationalDataStore
    def __init__(self, sqlParseString):
        # Create database connection
        try:
            print("Connecting to SQL Server source...")
            self.connect = pyodbc.connect(sqlParseString)
            self.cursor = self.connect.cursor()
            print("Database connected")
        except:
            print("Database connection failed")
            return

    def sqlGetSale(self):
        # Extract sale items (merged InternetSale, InternetSaleItem, StoreSale and SaleItem)
        internetSale = pd.read_sql_query('SELECT * FROM InternetSale',self.connect)
        internetSaleItem = pd.read_sql_query('SELECT * FROM InternetSaleItem',self.connect)
        internetOrders = internetSale.append(internetSaleItem, ignore_index=True)
        storeSale = pd.read_sql_query('SELECT * FROM StoreSale',self.connect)
        saleItem = pd.read_sql_query('SELECT * FROM SaleItem',self.connect)
        storeOrders = storeSale.append(saleItem, ignore_index=True)
        factSale = internetOrders.append(storeOrders, ignore_index=True)
        factSale.drop_duplicates(subset="SaleID", keep="first", inplace=True)
        factSale = factSale.rename(columns={"StaffID":"EmployeeID", "TaxRate":"SalesTax", "SaleTax":"SalesTax"})
        
        # Add to operational data store
        odsClass.df_FactSale = odsClass.df_FactSale.append(factSale, ignore_index=True)

        print("Sale Fact Table")
        print(factSale, "\n")
        print()

        self.sqlGetProduct()
        
    def sqlGetProduct(self):
        # Extract product dimension table
        dimProduct = pd.read_sql_query('SELECT * FROM Product',self.connect)
        dimProduct.drop_duplicates(subset="ProductID", keep="first", inplace=True)
        print("Product Dimension Table")

        # Add to operational data store
        odsClass.df_DimProduct = odsClass.df_DimProduct.append(dimProduct, ignore_index=True)

        print(dimProduct, "\n")
        print()
        
        self.sqlGetCustomer()

    def sqlGetCustomer(self):
        # Extract customer dimension table
        dimCustomer = pd.read_sql_query('SELECT * FROM Customer',self.connect)
        dimCustomer.drop_duplicates(subset="CustomerID", keep="first", inplace=True)

        print("Customer Dimension Table")
        print(dimCustomer, "\n")
        print()
        
        # Add to operational data store
        odsClass.df_DimCustomer = odsClass.df_DimCustomer.append(dimCustomer, ignore_index=True)

        self.sqlGetEmployee()

    def sqlGetEmployee(self):
        # Extract employee dimension table
        dimEmployee = pd.read_sql_query('SELECT * FROM Employee',self.connect)
        dimEmployee.drop_duplicates(subset="EmployeeID", keep="first", inplace=True)

        dimEmployee = dimEmployee.rename(columns={"Status":"StatusID", "Hiredate":"HireDate"})
        # Add to operational data store
        odsClass.df_DimEmployee = odsClass.df_DimEmployee.append(dimEmployee, ignore_index=True)

        print("Employee Dimension Table")
        print(dimEmployee, "\n")
        print()
        
        self.sqlGetEmployeeStatus()

    def sqlGetEmployeeStatus(self):
        # Extract employee status dimension table
        dimEmployeeStatus = pd.read_sql_query('SELECT * FROM EmployeeStatus',self.connect)
        dimEmployeeStatus.drop_duplicates(subset="StatusID", keep="first", inplace=True)

        # Add to operational data store
        odsClass.df_DimEmployeeStatus = odsClass.df_DimEmployeeStatus.append(dimEmployeeStatus)
        
        print("EmployeeStatus Dimension Table")
        print(dimEmployeeStatus, "\n")
        print()
        
        self.sqlGetJob()

    def sqlGetJob(self):
        # Extract job dimension table
        dimJob = pd.read_sql_query('SELECT * FROM Job',self.connect)
        dimJob.drop_duplicates(subset="JobTitleID", keep="first", inplace=True)

        # Add to operational data store
        odsClass.df_DimJob = odsClass.df_DimJob.append(dimJob, ignore_index=True)

        print("Job Dimension Table")
        print(dimJob, "\n")
        print()
       
        self.sqlGetStore()

    def sqlGetStore(self):
        # Extract store dimension table
        dimStore = pd.read_sql_query('SELECT * FROM Store',self.connect)
        dimStore.drop_duplicates(subset="StoreID", keep="first", inplace=True)

        # Add to operational data store
        odsClass.df_DimStore = odsClass.df_DimStore.append(dimStore, ignore_index=True)

        print("Store Dimension Table")
        print(dimStore, "\n")
        print()
        
        self.sqlGetSupplier()

    def sqlGetSupplier(self):
        # Extract supplier dimension table
        dimSupplier = pd.read_sql_query('SELECT * FROM Supplier',self.connect)
        dimSupplier.drop_duplicates(subset="SupplierID", keep="first", inplace=True)
        
        # Add to operational data store
        odsClass.df_DimSupplier = odsClass.df_DimSupplier.append(dimSupplier, ignore_index=True)

        print("Supplier Dimension Table")
        print(dimSupplier, "\n")