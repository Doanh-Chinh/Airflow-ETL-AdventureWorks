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

STG_DIM_GEOGRAPHY = os.getenv("STG_DIM_GEOGRAPHY")

def main() -> None:
    """Load geography dimension table"""
    # Get configs
    dir_path = "/data_lake"
    
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()

    df_src_address = spark.read.option("delimiter", "\t").csv(dir_path + "/Address.csv", schema = SCHEMAS.src_address)  # the option is need to avoding None return
    df_src_state_province = spark.read.option("delimiter", "\t").csv(dir_path + "/StateProvince.csv", schema = SCHEMAS.src_stateProvince)
    df_src_country_region = spark.read.option("delimiter", "\t").csv(dir_path + "/CountryRegion.csv", schema = SCHEMAS.src_countryRegion)
    df_src_address.show(5)
    df_src_state_province.show(5)
    df_src_country_region.show(5)

    # extract transform geography dimension
    df_dim_geography = preprocess_dim_geography(df_address=df_src_address, df_stateProvince=df_src_state_province, df_countryRegion=df_src_country_region)

    # Add dim_key column and rearrange columns order
    print("the dimension key will be created in post process then")
    df_dim_geography = df_dim_geography \
        .withColumn(
            "GeographyKey", 
            F.lit(None).cast("int")  # the dimension key will be created in post process then
        )
    # save as staging parquet
    df_dim_geography.show(10)   # for logging purposes
    df_dim_geography.printSchema()

    print(STG_DIM_GEOGRAPHY)
    df_dim_geography.write.parquet(STG_DIM_GEOGRAPHY, mode="overwrite")
    print('load_dim_geography_success')    
    print('*'*30)


def preprocess_dim_geography(df_address: DataFrame, df_stateProvince: DataFrame, df_countryRegion: DataFrame) -> DataFrame:
    spark = SparkSession.getActiveSession()
    # Registering DataFrame as temporary views
    df_address.createOrReplaceTempView("Address")
    df_stateProvince.createOrReplaceTempView("StateProvince")
    df_countryRegion.createOrReplaceTempView("CountryRegion")
    
    # Perform SQL query
    sql_query = """
        SELECT 
            a.AddressID as GeographyAK,
            a.AddressLine1,
	        a.AddressLine2,
            a.City,
            sp.StateProvinceCode,
            sp.Name as StateProvinceName,
            a.PostalCode,
            sp.CountryRegionCode,
            cr.Name as CountryRegionName
        FROM Address a
        JOIN StateProvince sp
            ON a.StateProvinceID = sp.StateProvinceID
        JOIN CountryRegion cr
            ON sp.CountryRegionCode = cr.CountryRegionCode
    """
    print(sql_query)
    # Execute the query
    sqlDF = spark.sql(sql_query)

    sqlDF.show()
    return sqlDF

if __name__ == "__main__":
    main() 
    