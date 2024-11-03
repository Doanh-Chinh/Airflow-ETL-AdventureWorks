from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import *
import pyspark.sql.functions as F

from typing import List
from datetime import datetime
import hdfs
import json
import os
print('='*30)
print(os.getcwd())
import argparse
import logging
log = logging.getLogger(__name__)

from config_services import ServiceConfig, get_default_SparkConf, SparkSchema
SCHEMAS = SparkSchema()
WEBHDFS_URI = ServiceConfig("webhdfs").uri

STG_FACT_SALES = os.getenv('STG_FACT_SALES')


def main() -> None:
    """Load stage  sales table"""
    # Get configs
    dir_path = "/data_lake"
    
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()

    df_src_product = spark.read.option("delimiter", "\t").csv(dir_path + "/Product.csv", schema = SCHEMAS.src_product)  # the option is need to avoding None return
    df_src_soHeader = spark.read.option("delimiter", "\t").csv(dir_path + "/SalesOrderHeader.csv", schema = SCHEMAS.src_soh)
    df_src_soDetail = spark.read.option("delimiter", "\t").csv(dir_path + "/SalesOrderDetail.csv", schema = SCHEMAS.src_sod)


    df_src_product.show(5)
    df_src_soHeader.show(5)
    df_src_soDetail.show(5)

    # extract transform  sales
    df_stage_sales = preprocess_stage_sales(df_product=df_src_product, df_soHeader=df_src_soHeader, df_soDetail=df_src_soDetail)


    # save as staging parquet
    df_stage_sales.show(10)   # for logging purposes
    df_stage_sales.printSchema()

    print(STG_FACT_SALES)
    df_stage_sales.write.parquet(STG_FACT_SALES, mode="overwrite")
    print('load_stg_fact_sales_success')    
    print('*'*30)


def preprocess_stage_sales(df_product: DataFrame, df_soHeader: DataFrame, df_soDetail: DataFrame) -> DataFrame:
    spark = SparkSession.getActiveSession()
    # Registering DataFrame as temporary views
    df_product.createOrReplaceTempView("Product")
    df_soHeader.createOrReplaceTempView("SalesOrderHeader")
    df_soDetail.createOrReplaceTempView("SalesOrderDetail")
    
    # Perform SQL query
    sql_query = """
        SELECT soh.SalesOrderNumber,
            ROW_NUMBER() over(partition by sod.SalesOrderID order by sod.SalesOrderDetailID) as SalesOrderLineNumber,
                soh.RevisionNumber,
                sod.ProductID as ProductAK,
                soh.CustomerID as CustomerAK,
                soh.SalesPersonID as EmployeeAK,
                sod.SpecialOfferID as PromotionAK,
                soh.TerritoryID as SalesTerritoryAK,
                soh.BillToAddressID as BillToAddressAK,
                soh.ShipToAddressID as ShipToAddressAK,
                year(soh.OrderDate)*10000 + month(soh.OrderDate)*100 + day(soh.OrderDate) as OrderDateKey,
                year(soh.ShipDate)*10000 + month(soh.ShipDate)*100 + day(soh.ShipDate) as ShipDateKey,
                year(soh.DueDate)*10000 + month(soh.DueDate)*100 + day(soh.DueDate) as DueDateKey,
                sod.OrderQty as OrderQuantity,
                sod.UnitPrice,
                sod.UnitPriceDiscount as DiscountAmount,
                p.StandardCost as ProductStandardCost,
                p.StandardCost * sod.OrderQty as TotalProductCost,
                sod.LineTotal as SalesAmount,
                soh.TaxAmt/soh.SubTotal * sod.LineTotal as TaxAmount,
                soh.Freight/soh.SubTotal * sod.LineTotal as FreightAmount
        FROM SalesOrderHeader soh
        JOIN SalesOrderDetail sod
            ON soh.SalesOrderID = sod.SalesOrderID
        JOIN Product p
            ON sod.ProductID = p.ProductID
    """
    print(sql_query)
    # Execute the query
    sqlDF = spark.sql(sql_query)

    sqlDF.show()
    return sqlDF
    return df


if __name__ == "__main__":
    main() 


