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

STG_DIM_SALES_TERRITORY = os.getenv('STG_DIM_SALES_TERRITORY')

def main() -> None:
    """Load sales_territory dimension table"""
    # Get configs
    dir_path = "/data_lake"
    
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()

    df_src_sales_territory = spark.read.option("delimiter", "\t").csv(dir_path + "/SalesTerritory.csv", schema = SCHEMAS.src_salesTerritory)  # the option is need to avoding None return
    df_src_country_region = spark.read.option("delimiter", "\t").csv(dir_path + "/CountryRegion.csv", schema = SCHEMAS.src_countryRegion)
    df_src_sales_territory.show(5)
    df_src_country_region.show(5)

    # extract transform sales_territory dimension
    df_dim_sales_territory = preprocess_dim_sales_territory(df_sales_territory=df_src_sales_territory, df_countryRegion=df_src_country_region)

    # Add dim_key column and rearrange columns order
    print("the dimension key will be created in post process then")
    df_dim_sales_territory = df_dim_sales_territory \
        .withColumn(
            "SalesTerritoryKey", 
            F.lit(None).cast("int")  # the dimension key will be created in post process then
        )
    # save as staging parquet
    df_dim_sales_territory.show(10)   # for logging purposes
    df_dim_sales_territory.printSchema()

    print(STG_DIM_SALES_TERRITORY)
    df_dim_sales_territory.write.parquet(STG_DIM_SALES_TERRITORY, mode="overwrite")
    print('load_dim_sales_territory_success')    
    print('*'*30)


def preprocess_dim_sales_territory(df_sales_territory: DataFrame, df_countryRegion: DataFrame) -> DataFrame:
    spark = SparkSession.getActiveSession()
    # Registering DataFrame as temporary views
    df_sales_territory.createOrReplaceTempView("SalesTerritory")
    df_countryRegion.createOrReplaceTempView("CountryRegion")
    
    # Perform SQL query
    sql_query = """
        SELECT 
            st.TerritoryID as SalesTerritoryAK,
            st.Name as SalesTerritoryName,
            st.CountryRegionCode as SalesCountryRegionCode,
            cr.Name as SalesCountryRegionName
        FROM SalesTerritory st
        JOIN CountryRegion cr
            ON st.CountryRegionCode = cr.CountryRegionCode
    """
    print(sql_query)
    # Execute the query
    sqlDF = spark.sql(sql_query)

    sqlDF.show()
    return sqlDF

if __name__ == "__main__":
    main() 
    