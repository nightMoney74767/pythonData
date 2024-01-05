# This class exports all the data from the operational data store to a new SQL database
import chunk
from logging import exception
import pyodbc
import operationalDataStore

class exportData:
    # This string is used to build the tables in the SQL database
    exportBuilder = ''' /* This script builds the tables in a new SQL database, which stores data exported from the operational data store */
DROP TABLE IF EXISTS FactSale
DROP TABLE IF EXISTS DimSupplier
DROP TABLE IF EXISTS DimProduct
DROP TABLE IF EXISTS DimStore
DROP TABLE IF EXISTS DimJob
DROP TABLE IF EXISTS DimEmployee
DROP TABLE IF EXISTS DimEmployeeStatus
DROP TABLE IF EXISTS DimCustomer

CREATE TABLE DimCustomer(
    CustomerID nvarchar(250),
    CustomerEmail nvarchar(250),
    FirstName nvarchar(250),
    SecondName  nvarchar(250),
    CustomerType  nvarchar(250),
    City nvarchar(250),
    StateProvince nvarchar(250),
    Country nvarchar(250),
    PostalCode nvarchar(250),

	CONSTRAINT PKCustomerID PRIMARY KEY (CustomerID)
)

CREATE TABLE DimEmployeeStatus(
    StatusID nvarchar(15),
	StatusDescription nvarchar(250),

	CONSTRAINT PKStatusID PRIMARY KEY (StatusID)
)

CREATE TABLE DimEmployee(
    EmployeeID nvarchar(250),
	FirstName nvarchar(250),
	LastName nvarchar(250),
	BirthDate date,
	Hiredate date,
	EndDate date,
	EmailAddress nvarchar(250),
	Phone nvarchar(11),
	EmergencyContactPhone nvarchar(11),
	StatusID nvarchar(15),
	StoreID nvarchar(250)

	CONSTRAINT PKEmployeeID PRIMARY KEY (EmployeeID),
	CONSTRAINT FKStatusID FOREIGN KEY (StatusID) REFERENCES DimEmployeeStatus (StatusID)
)

CREATE TABLE DimJob(
    JobTitleID nvarchar(250),
	JobTitle nvarchar(250),
	JobDescription nvarchar(500),
	Salaried bit,
	PayRate money,
	PayFrequencyID int,
	VacationHours int,
	SickLeaveAllowance int,
	SalesPersonFlag bit,

	CONSTRAINT PKJobTitleID PRIMARY KEY (JobTitleID)
)

CREATE TABLE DimStore(
    StoreID nvarchar(250),
	StoreAddress nvarchar(250),
	StoreCity nvarchar(250),
	StoreStateProvince nvarchar(250),
	StoreCountry nvarchar(250),
	StorePostCode nvarchar(10),
	StorePhone nvarchar(11),

	CONSTRAINT PKStoreID PRIMARY KEY (StoreID)
)

CREATE TABLE DimProduct(
    ProductID nvarchar(150),
	ProductDescription nvarchar(500),
	CategoryID nvarchar(15),
	SupplierPrice money,
	ProductPrice money,
	SafetyStockLevel decimal,
	ReorderPoint decimal,
    SupplierID nvarchar(250),

	CONSTRAINT PKProductID PRIMARY KEY (ProductID)
)

CREATE TABLE DimSupplier(
    SupplierID nvarchar(250),
	SupplierAddress nvarchar(250),
	SupplierCity nvarchar(250),
	SupplierStateProvince nvarchar(250),
	SupplierCountry nvarchar(250),
	SupplierPostCode nvarchar(10),
	SupplierPhone nvarchar(11),

	CONSTRAINT PKSupplierID PRIMARY KEY (SupplierID)
)

CREATE TABLE FactSale(
    SaleID nvarchar(250),
    ProductID nvarchar(150),
    CustomerID nvarchar(250),
    DateOfSale date,
    Quantity decimal,
    SaleAmount money,
    SalesTax money,
    SaleTotal money,
    StoreID nvarchar(250),
    DateShipped date,
    ShippingType nvarchar(50),
    EmployeeID nvarchar(250),
	SupplierID nvarchar(250),
	JobTitleID nvarchar(250),

	CONSTRAINT PKSaleID PRIMARY KEY (SaleID),
	CONSTRAINT FKProductID FOREIGN KEY (ProductID) REFERENCES DimProduct (ProductID),
	CONSTRAINT FKCustomerID FOREIGN KEY (CustomerID) REFERENCES DimCustomer (CustomerID),
	CONSTRAINT FKStoreID FOREIGN KEY (StoreID) REFERENCES DimStore (StoreID),
	CONSTRAINT FKEmployeeID FOREIGN KEY (EmployeeID) REFERENCES DimEmployee (EmployeeID),
	CONSTRAINT FKSupplierID FOREIGN KEY (SupplierID) REFERENCES DimSupplier (SupplierID),
	CONSTRAINT FKJobTitleID FOREIGN KEY (JobTitleID) REFERENCES DimJob (JobTitleID)
)
    '''

    def __init__(self, exportString):
        # Initiate a connection to SQL database
        try:
            print("Connecting to SQL export database...")
            self.connect = pyodbc.connect(exportString)
            self.cursor = self.connect.cursor()
            print("Database connected")
        except:
            print("Database connection failed")
            return
    
    def buildExportTables(self):
        # Build tables using the string exportBuilder
        print("Attempting to execute script to build SQL tables...")
        self.cursor.execute(self.exportBuilder)
        self.connect.commit()
        print("Tables built")

    def exportUsingCsv(self, table, dataFrame, chunkSize):
        # Create a string that consists of the data frame columns. This string is added to the insert into command below
        # TODO: Create strings listing the columns of each table. Use if statements to determine which one to add to the SQL insert into command below. Each dataframe ends up with additional columns, preventing any export to SQL
        # This prevents the SQL error 213 where "column names or [the] number of supplied values does not match [a] table definition" (Ian, 2020)
        factSaleColumns = "SaleID, ProductID, CustomerID, DateOfSale, Quantity, SaleAmount, SalesTax, SaleTotal, StoreID, DateShipped, ShippingType, EmployeeID, SupplierID, JobTitleID"
        dimCustomerColumns = "CustomerID, CustomerEmail, FirstName, SecondName, CustomerType, City, StateProvince, Country, PostalCode"
        dimEmployeeColumns = "EmployeeID, FirstName, LastName, BirthDate, HireDate, EndDate, EmailAddress, Phone, EmergencyContactPhone, StatusID, StoreID"
        dimEmployeeStatusColumns = "StatusID, StatusDescription"
        dimJobColumns = "JobTitleID, JobTitle, JobDescription, Salaried, PayRate, VacationHours, SickLeaveAllowance, SalesPersonFlag"
        dimStoreColumns = "StoreID, StoreAddress, StoreCity, StoreStateProvince, StoreCountry, StorePostCode, StorePhone"
        dimProductColumns = "ProductID, ProductDescription, CategoryID, SupplierPrice, ProductPrice, SafetyStockLevel, ReorderPoint, SupplierID"
        dimSupplierColumns = "SupplierID, SupplierAddress, SupplierCity, SupplierStateProvince, SupplierCountry, SupplierPostCode, SupplierPhone"
        
        columns = ""
        if table == "FactSale":
            columns = factSaleColumns
        if table == "DimCustomer":
            columns = dimCustomerColumns
        if table == "DimEmployee":
            columns = dimEmployeeColumns
        if table == "DimEmployeeStatus":
            columns = dimEmployeeStatusColumns
        if table == "DimJob":
            columns = dimJobColumns
        if table == "DimStore":
            columns = dimStoreColumns
        if table == "DimProduct":
            columns = dimProductColumns
        if table == "DimSupplier":
            columns = dimSupplierColumns

        # Export data to SQL database
        rows = len(dataFrame.index)
        current = 0
        while current < rows:
            if rows-current < chunkSize:
                stop = rows
            else:
                stop = current + chunkSize
            
            csv = dataFrame.iloc[current:stop].to_csv(index = False, header = False, quoting = 1, quotechar="'", line_terminator="),\n(")
            csv = csv[:-3]
            values = f"({csv}"
            sql = f"INSERT INTO {table} ({columns}) VALUES {values}".replace("''", 'NULL')
            print("\t\tRows remaining:", rows - current, "\tInsert", values, "into", table)
            current = stop
            self.cursor.execute(sql)
        self.connect.commit()

    def exportOperationalDataStore(self): 
        # Exort operational data store to SQL database
        print("Attempting to export the operational data store to SQL database...")
        chunkSize = 1
        for table in operationalDataStore.tables:
            dataFrame = getattr(operationalDataStore.odsClass, f"df_{table}")
            print(f"\tAttempting to export the dataframe df_{table} to SQL database as {table} with {len(dataFrame.index)} rows...")
            self.exportUsingCsv(table, dataFrame, chunkSize)
            print("Table exported")