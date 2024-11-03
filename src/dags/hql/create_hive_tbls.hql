-- Create the Meta schema if it does not exist
CREATE SCHEMA IF NOT EXISTS Meta;

-- Create the Meta.DataFlow table if it does not exist
CREATE TABLE IF NOT EXISTS Meta.DataFlow (
    DataFlowName STRING NOT NULL, 
    LastSuccessfulModifiedDate TIMESTAMP    
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- Display message
-- (For logging or to check if the table exists, you can run the following query)
DESCRIBE FORMATTED Meta.DataFlow;

INSERT INTO Meta.DataFlow (DataFlowName, LastSuccessfulModifiedDate)
VALUES ('DimCustomer', NULL);

-- DimCustomer
CREATE TABLE IF NOT EXISTS DimCustomer (
    CustomerKey INT NOT NULL,
    CustomerAK INT NOT NULL,  -- CustomerID
    Title STRING,
    FirstName STRING,
    MiddleName STRING,
    LastName STRING,
    Suffix STRING,
    EmailAddress STRING,
    ExpirationDate TIMESTAMP,
    LoadDate TIMESTAMP,
    Inferred TINYINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- DimProduct
CREATE TABLE IF NOT EXISTS DimProduct (
    ProductKey INT NOT NULL,
    ProductAK INT NOT NULL,  -- ProductID
    ProductName STRING,
    ProductNumber STRING,
    ProductSubCategoryName STRING,
    ProductCategoryName STRING,
    FinishedGoodsFlag BOOLEAN,
    Color STRING,
    StandardCost DECIMAL(10,2),
    ListPrice DECIMAL(10,2),
    Size STRING,
    ProductLine STRING,
    Class STRING,
    Style STRING,
    ProductModelName STRING,
    SellStartDate TIMESTAMP,
    SellEndDate TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- DimPromotion
CREATE TABLE IF NOT EXISTS DimPromotion (
    PromotionKey INT NOT NULL,
    PromotionAK INT,  -- SpecialOfferID
    PromotionName STRING,  -- Description
    DiscountPercentage DECIMAL(10,2),  -- DiscountPct
    PromotionType STRING,  -- Type
    PromotionCategory STRING,
    PromotionStartDate TIMESTAMP,
    PromotionEndDate TIMESTAMP,
    MinQuantity INT,
    MaxQuantity INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- DimSalesTerritory
CREATE TABLE IF NOT EXISTS DimSalesTerritory (
    SalesTerritoryKey INT NOT NULL,
    SalesTerritoryAK INT,  -- TerritoryID
    SalesTerritoryName STRING,
    SalesCountryRegionCode STRING,
    SalesCountryRegionName STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- DimGeography
CREATE TABLE IF NOT EXISTS DimGeography (
    GeographyKey INT NOT NULL,
    GeographyAK INT NOT NULL,
    City STRING,
    AddressLine1 STRING,
	AddressLine2 STRING,
    StateProvinceCode STRING,
    StateProvinceName STRING,
    PostalCode STRING,
    CountryRegionCode STRING,
    CountryRegionName STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- DimEmployee
CREATE TABLE IF NOT EXISTS DimEmployee (
    EmployeeKey INT NOT NULL,
    EmployeeAK STRING,
    Title STRING,
    FirstName STRING,
    LastName STRING,
    MiddleName STRING,
    Suffix STRING,
    EmailAddress STRING,
    JobTitle STRING,
    CurrentFlag BOOLEAN,
    ExpirationDate TIMESTAMP,
    LoadDate TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- FactSales
CREATE TABLE IF NOT EXISTS FactSales (
    SalesOrderNumber STRING NOT NULL,
    SalesOrderLineNumber TINYINT NOT NULL,
    RevisionNumber INT NOT NULL,
    ProductKey INT NOT NULL,
    SalesPersonKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    BillToAddressKey INT NOT NULL,
    ShipToAddressKey INT NOT NULL,
    PromotionKey INT NOT NULL,
    SalesTerritoryKey INT NOT NULL,
    OrderDateKey INT NOT NULL,
    DueDateKey INT NOT NULL,
    ShipDateKey INT NOT NULL,
    OrderQuantity SMALLINT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    DiscountAmount FLOAT NOT NULL,
    ProductStandardCost DECIMAL(10,2) NOT NULL,
    TotalProductCost DECIMAL(10,2) NOT NULL,
    SalesAmount DECIMAL(10,2) NOT NULL,
    TaxAmount DECIMAL(10,2) NOT NULL,
    FreightAmount DECIMAL(10,2) NOT NULL
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- DimDate
CREATE TABLE IF NOT EXISTS DimDate (
    date_key INT NOT NULL,
    date_date DATE,
    year INT,
    month INT,
    day INT,
    week_of_year INT,
    day_of_week INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
