"""File for DAGs related configs"""

from pyspark.conf import SparkConf
from pyspark.sql.types import *


"""Service addresses and URIs"""
class ServiceConfig:
    """Class for service configs"""
    def __init__(self, key: str):
        lookup = {
            "hdfs": ("hdfs", "namenode", "8020"),
            "webhdfs": ("http", "namenode", "9870"),
            "spark": ("spark", "spark-master", "7077"),
            "hive_metastore": ("thrift", "hive-metastore", "9083")
        }
        try:
            self.service, self.hostname, self.port = lookup[key]
        except KeyError:
            raise ValueError("Invalid input key.")

    @property
    def addr(self):
        return f"{self.hostname}:{self.port}"

    @property
    def uri(self):
        return f"{self.service}://{self.addr}"


"""Spark configs and schemas"""
def get_default_SparkConf() -> SparkConf:
    """Get a SparkConf object with some default values"""
    HDFS_CONF = ServiceConfig("hdfs")
    SPARK_CONF = ServiceConfig("spark")
    HIVE_METASTORE_CONF = ServiceConfig("hive_metastore")

    default_configs = [
        ("spark.master", SPARK_CONF.uri),
        ("spark.sql.warehouse.dir", HDFS_CONF.uri + "/data_warehouse"),
        (
            "spark.hadoop.fs.defaultFS",
            f"{HDFS_CONF.service}://{HDFS_CONF.hostname}"
        ),
        ("spark.hadoop.dfs.replication", 1),
        ("spark.hive.exec.dynamic.partition", True),
        ("spark.hive.exec.dynamic.partition.mode", "nonstrict"),  
        ("spark.hive.metastore.uris", HIVE_METASTORE_CONF.uri),  # so that Spark can read metadata into catalog
        ("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh"),
        # ("spark.sql.hive.exec.local.time.zone", "Asia/Ho_Chi_Minh"),
        # ("spark.sql.hive.exec.local.timezone", "Asia/Ho_Chi_Minh")
    ]
        
    conf = SparkConf().setAll(default_configs)
    return conf


class SparkSchema:
    """Contains PySpark schemas used in DAGs"""
    def __init__(self):
        # Source schema expected for promotion
        self.src_promotion = StructType(
            [
                StructField("SpecialOfferID", IntegerType(), False),
                StructField("Description", StringType(), True),
                StructField("DiscountPct", FloatType(), True),
                StructField("Type", StringType(), True),
                StructField("Category", StringType(), True),
                StructField("StartDate", DateType(), True),
                StructField("EndDate", DateType(), True),
                StructField("MinQty", IntegerType(), True),
                StructField("MaxQty", IntegerType(), True),
                StructField("rowguid", StringType(), True), 
                StructField("ModifiedDate", DateType(), True)
            ]
        )
        self.src_employee = StructType(
            [
                StructField("BusinessEntityID", IntegerType(), nullable=False),
                StructField("NationalIDNumber", StringType(), nullable=False),     # nvarchar(15)
                StructField("LoginID", StringType(), nullable=False),              # nvarchar(256)
                StructField("OrganizationNode", StringType(), nullable=True),      # hierarchyid - use StringType for hierarchical data
                StructField("OrganizationLevel", IntegerType(), nullable=True),    # int or nullable if data is not always present
                StructField("JobTitle", StringType(), nullable=False),             # nvarchar(50)
                StructField("BirthDate", DateType(), nullable=False),              # date
                StructField("MaritalStatus", StringType(), nullable=False),        # nchar(1)
                StructField("Gender", StringType(), nullable=False),               # nchar(1)
                StructField("HireDate", DateType(), nullable=False),               # date
                StructField("SalariedFlag", BooleanType(), nullable=False),        # bit (True/False)
                StructField("VacationHours", ShortType(), nullable=False),         # smallint
                StructField("SickLeaveHours", ShortType(), nullable=False),        # smallint
                StructField("CurrentFlag", BooleanType(), nullable=False),         # bit (True/False)
                StructField("rowguid", StringType(), nullable=False),              # uniqueidentifier, stored as a string
                StructField("ModifiedDate", TimestampType(), nullable=False)       # datetime
            ]
        )    
        self.src_product = StructType(
            [
                StructField("ProductID", IntegerType(), False),
                StructField("Name", StringType(), False),
                StructField("ProductNumber", StringType(), False),
                StructField("MakeFlag", IntegerType(), False),
                StructField("FinishedGoodsFlag", BooleanType(), False),
                StructField("Color", StringType(), True),
                StructField("SafetyStockLevel", IntegerType(), False),
                StructField("ReorderPoint", IntegerType(), False),
                StructField("StandardCost", FloatType(), False),
                StructField("ListPrice", FloatType(), False),
                StructField("Size", StringType(), True),
                StructField("SizeUnitMeasureCode", StringType(), True),
                StructField("WeightUnitMeasureCode", StringType(), True),
                StructField("Weight", FloatType(), True),
                StructField("DaysToManufacture", IntegerType(), False),
                StructField("ProductLine", StringType(), True),
                StructField("Class", StringType(), True),
                StructField("Style", StringType(), True),
                StructField("ProductSubcategoryID", FloatType(), True),
                StructField("ProductModelID", FloatType(), True),
                StructField("SellStartDate", DateType(), False),
                StructField("SellEndDate", DateType(), True),
                StructField("DiscontinuedDate", DateType(), True),
                StructField("rowguid", StringType(), False),
                StructField("ModifiedDate", DateType(), False)
            ]
        )
        self.src_productSubCategory = StructType([
                StructField("ProductSubcategoryID", IntegerType(), False),
                StructField("ProductCategoryID", IntegerType(), False),
                StructField("Name", StringType(), False),
                StructField("rowguid", StringType(), False),
                StructField("ModifiedDate", DateType(), False)
            ])
        self.src_productCategory = StructType([
                StructField("ProductCategoryID", IntegerType(), False),
                StructField("Name", StringType(), False),
                StructField("rowguid", StringType(), False),
                StructField("ModifiedDate", DateType(), False)
            ])
            # Define the schema for the ProductModel table
        self.src_productModel = StructType([
            StructField("ProductModelID", IntegerType(), nullable=False),
            StructField("Name", StringType(), nullable=True),
            StructField("CatalogDescription", StringType(), nullable=True),  # Assuming XML content is stored as a string
            StructField("Instructions", StringType(), nullable=True),        # Assuming XML content is stored as a string
            StructField("rowguid", StringType(), nullable=False),            # Uniqueidentifier as String
            StructField("ModifiedDate", TimestampType(), nullable=False)
        ])
        # Define the schema for Customer
        self.src_customer = StructType([
            StructField("CustomerID", IntegerType(), nullable=False),  # Unchecked
            StructField("PersonID", FloatType(), nullable=True),  # Checked
            StructField("StoreID", FloatType(), nullable=True),   # Checked
            StructField("TerritoryID", FloatType(), nullable=True),  # Checked
            StructField("AccountNumber", StringType(), nullable=False),  # Unchecked
            StructField("rowguid", StringType(), nullable=False),  # Unchecked
            StructField("ModifiedDate", DateType(), nullable=False)  # Unchecked (assuming datetime maps to DateType in Spark)
        ])
        # Define the schema for Person
        self.src_person= StructType([
            StructField("BusinessEntityID", IntegerType(), nullable=False),  # Unchecked
            StructField("PersonType", StringType(), nullable=False),  # Unchecked
            StructField("NameStyle", IntegerType(), nullable=False),  # Unchecked (assuming NameStyle:bit maps to BooleanType)
            StructField("Title", StringType(), nullable=True),  # Checked
            StructField("FirstName", StringType(), nullable=False),  # Unchecked
            StructField("MiddleName", StringType(), nullable=True),  # Checked
            StructField("LastName", StringType(), nullable=False),  # Unchecked
            StructField("Suffix", StringType(), nullable=True),  # Checked
            StructField("EmailPromotion", IntegerType(), nullable=False),  # Unchecked
            StructField("AdditionalContactInfo", StringType(), nullable=True),  # Checked (assuming xml is stored as StringType)
            StructField("Demographics", StringType(), nullable=True),  # Checked (assuming xml is stored as StringType)
            StructField("rowguid", StringType(), nullable=False),  # Unchecked
            StructField("ModifiedDate", TimestampType(), nullable=False)  # Unchecked (assuming datetime maps to TimestampType in Spark)
        ])
        # Define the schema for EmailAddress
        self.src_emailAddress = StructType([
            StructField("BusinessEntityID", IntegerType(), nullable=False),  # Unchecked
            StructField("EmailAddressID", IntegerType(), nullable=False),  # Unchecked
            StructField("EmailAddress", StringType(), nullable=True),  # Checked
            StructField("rowguid", StringType(), nullable=False),  # Unchecked
            StructField("ModifiedDate", TimestampType(), nullable=False)  # Unchecked (assuming datetime maps to TimestampType in Spark)
        ])

        self.src_salesTerritory = StructType([
            StructField("TerritoryID", IntegerType(), nullable=False),
            StructField("Name", StringType(), nullable=False),
            StructField("CountryRegionCode", StringType(), nullable=False),
            StructField("[Group]", StringType(), nullable=False),
            StructField("SalesYTD", FloatType(), nullable=False),
            StructField("SalesLastYear", FloatType(), nullable=False),
            StructField("CostYTD", FloatType(), nullable=False),
            StructField("CostLastYear", FloatType(), nullable=False),
            StructField("rowguid", StringType(), nullable=False),
            StructField("ModifiedDate", DateType(), nullable=False)
        ])

        self.src_countryRegion = StructType([
            StructField("CountryRegionCode", StringType(), nullable=False),
            StructField("Name", StringType(), nullable=False),
            StructField("ModifiedDate", TimestampType(), nullable=False)
        ])

        self.src_address = StructType([
            StructField("AddressID", IntegerType(), nullable=False),
            StructField("AddressLine1", StringType(), nullable=False),
            StructField("AddressLine2", StringType(), nullable=True),  # Checked means nullable
            StructField("City", StringType(), nullable=False),
            StructField("StateProvinceID", IntegerType(), nullable=False),
            StructField("PostalCode", StringType(), nullable=False),
            StructField("SpatialLocation", StringType(), nullable=True),  # Using StringType for geography
            StructField("rowguid", StringType(), nullable=False),
            StructField("ModifiedDate", TimestampType(), nullable=False)
        ])

        self.src_stateProvince = StructType([
            StructField("StateProvinceID", IntegerType(), nullable=False),
            StructField("StateProvinceCode", StringType(), nullable=False),
            StructField("CountryRegionCode", StringType(), nullable=False),
            StructField("IsOnlyStateProvinceFlag", IntegerType(), nullable=False),  # bit to BooleanType
            StructField("Name", StringType(), nullable=False),
            StructField("TerritoryID", IntegerType(), nullable=False),
            StructField("rowguid", StringType(), nullable=False),  # uniqueidentifier to StringType
            StructField("ModifiedDate", TimestampType(), nullable=False)
        ]) 

        self.src_soh = StructType([
            StructField("SalesOrderID", IntegerType(), nullable=False),
            StructField("RevisionNumber", IntegerType(), nullable=False),  # Replaced ByteType() with IntegerType()
            StructField("OrderDate", TimestampType(), nullable=False),
            StructField("DueDate", TimestampType(), nullable=False),
            StructField("ShipDate", TimestampType(), nullable=True),  # Checked means nullable
            StructField("Status", IntegerType(), nullable=False),  # Replaced ByteType() with IntegerType()
            StructField("OnlineOrderFlag", IntegerType(), nullable=False),
            StructField("SalesOrderNumber", StringType(), nullable=True),
            StructField("PurchaseOrderNumber", StringType(), nullable=True),
            StructField("AccountNumber", StringType(), nullable=True),
            StructField("CustomerID", IntegerType(), nullable=False),
            StructField("SalesPersonID", IntegerType(), nullable=True),
            StructField("TerritoryID", IntegerType(), nullable=True),
            StructField("BillToAddressID", IntegerType(), nullable=False),
            StructField("ShipToAddressID", IntegerType(), nullable=False),
            StructField("ShipMethodID", IntegerType(), nullable=False),
            StructField("CreditCardID", IntegerType(), nullable=True),
            StructField("CreditCardApprovalCode", StringType(), nullable=True),
            StructField("CurrencyRateID", IntegerType(), nullable=True),
            StructField("SubTotal", FloatType(), nullable=False),  # Replaced DecimalType() with FloatType()
            StructField("TaxAmt", FloatType(), nullable=False),
            StructField("Freight", FloatType(), nullable=False),
            StructField("TotalDue", FloatType(), nullable=True),
            StructField("Comment", StringType(), nullable=True),
            StructField("rowguid", StringType(), nullable=False),
            StructField("ModifiedDate", TimestampType(), nullable=False)
        ])

        self.src_sod = StructType([
            StructField("SalesOrderID", IntegerType(), nullable=False),
            StructField("SalesOrderDetailID", IntegerType(), nullable=False),
            StructField("CarrierTrackingNumber", StringType(), nullable=True),  # Checked means nullable
            StructField("OrderQty", ShortType(), nullable=False),  # smallint to ShortType
            StructField("ProductID", IntegerType(), nullable=False),
            StructField("SpecialOfferID", IntegerType(), nullable=False),
            StructField("UnitPrice", FloatType(), nullable=False),  # money to FloatType
            StructField("UnitPriceDiscount", FloatType(), nullable=False),  # money to FloatType
            StructField("LineTotal", FloatType(), nullable=True),  # Unchecked means nullable
            StructField("rowguid", StringType(), nullable=False),  # uniqueidentifier to StringType
            StructField("ModifiedDate", TimestampType(), nullable=False)
        ])