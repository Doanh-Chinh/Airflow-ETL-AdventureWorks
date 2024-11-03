from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import *
import pyspark.sql.functions as F
import sys
from pathlib import Path

# Add the plugins path to the system path
sys.path.append(str(Path('/opt/airflow/plugins')))
from utils.utils import _create_new_dimension_with_new_key

from typing import List
from datetime import datetime
import hdfs
import json
import os
print('='*30)
print(os.getcwd())
import argparse
import logging

from config_services import ServiceConfig, get_default_SparkConf, SparkSchema
SCHEMAS = SparkSchema()
WEBHDFS_URI = ServiceConfig("webhdfs").uri

STG_FACT_SALES = os.getenv('STG_FACT_SALES')
INFERRED_MEMBER_PATH = os.getenv('INFERRED_MEMBER_PATH')

def main() -> None:
    """Load fact sales table"""
    # Get configs
    dir_path = "/data_lake"
    destination_name = 'FactSales'
    print('Fact name:', destination_name)
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()
    print('#'*30)
    # Read the staging  sales
    df_stg_fact_sales = spark.read.parquet(STG_FACT_SALES)
    print('Read staging file ok...')
    # Check if new  sales records compare to fact  sales table based on SalesOrderNumber key
    # load fact  sales table
    df_fact_sales = spark.table(destination_name)
    print('Read fact table ok...')

    # fact destination compares to staging  sales to see what new
    df_new_sales, df_new_count = get_new_sales(stg=df_stg_fact_sales, des=df_fact_sales, key='SalesOrderNumber')
    # if new found, then lookup dim key by dim alternative key, after then insert append into fact destination. Otherwise,
    # print Not having news to insert
    if df_new_count > 0:
        print('Inserting new records in fact...')
        print('*'*30)

        # Read the dimensions table into a DataFrame
        df_dim_customer, df_dim_employee, df_dim_promotion, df_dim_product, df_dim_geography, df_dim_sales_territory = load_dimensions(
            'DimCustomer',
            'DimEmployee',
            'DimPromotion',
            'DimProduct',
            'DimGeography',
            'DimSalesTerritory'
        )
        # lst_df_dim_customer = load_dimensions(
        #     'DimCustomer'
        # )
        # df_dim_customer = lst_df_dim_customer[0]

        # Join dimensions to get dim_key based on dim_alternative_key in fact table
        df_fact_sales = df_new_sales \
        .transform(lambda df: lookup_dim_key(df, df_dim_customer, "CustomerKey", "CustomerAK")) \
        .transform(lambda df: lookup_dim_key(df, df_dim_employee, "EmployeeKey", "EmployeeAK")) \
        .transform(lambda df: lookup_dim_key(df, df_dim_promotion, "PromotionKey", "PromotionAK")) \
        .transform(lambda df: lookup_dim_key(df, df_dim_product, "ProductKey", "ProductAK")) \
        .transform(lambda df: lookup_dim_key(df, df_dim_geography, "BillToAddressKey", "BillToAddressAK")) \
        .transform(lambda df: lookup_dim_key(df, df_dim_geography, "ShipToAddressKey", "ShipToAddressAK")) \
        .transform(lambda df: lookup_dim_key(df, df_dim_sales_territory, "SalesTerritoryKey", "SalesTerritoryAK"))

        # df_fact_sales = df_new_sales.transform(lambda df: lookup_dim_key(df, df_dim_customer, "CustomerKey", "CustomerAK"))
        # rename col for mapping with destination schema
        df_fact_sales = df_fact_sales.withColumnsRenamed({'EmployeeKey': 'SalesPersonKey'})

        # Verify the final schema and mappings
        print('fact_df schema')
        df_fact_sales.printSchema()
        df_fact_sales.show()
        
        # Save fact table
        df_fact_sales.write \
            .mode("append") \
            .format("hive") \
            .saveAsTable(destination_name)
        print('*'*30)
        print(f"{df_new_count} new records inserted into {destination_name}")

    else:
        print('*'*30)
        print("No new data from staging source comparing to fact destination!")

def load_dimensions(*dim_name):
    """
    Read the dimensions table in current SparkSession
    
    Parameters:
    - *dim_name: dimension names seperated by comma

    Return: 
    - List: list of dimension tables
    """
    spark = SparkSession.getActiveSession()
    lst_dims = []
    for name in dim_name:
        dim = spark.table(name)
        lst_dims.append(dim)
    print('List of dimensions:', lst_dims)
    return lst_dims


def lookup_dim_key(fact_df: DataFrame, dim_df: DataFrame, dim_key: str, dim_alt_key: str) -> DataFrame:
    """
    This function joins a dimension DataFrame with the fact DataFrame based on the alternative key
    and returns the updated fact DataFrame with the dim_key column.

    Parameters:
    - fact_df (DataFrame): The fact table DataFrame (e.g., FactSales).
    - dim_df (DataFrame): The dimension table DataFrame (e.g., DimProduct).
    - dim_key (str): The primary key column in the dimension table (e.g., ProductKey).
    - dim_alt_key (str): The alternative key column in the dimension table (e.g., ProductAK).
    
    Returns:
    - DataFrame: The fact DataFrame with the new dimension key column added.
    """

    # special process for look up dim geography causing by dim alternative key name
    if dim_alt_key == 'BillToAddressAK':
        # temporarily rename GeographyAK into BillToAddressAK in dim
        dim_df = dim_df.withColumnsRenamed(
            {
                "GeographyAK": "BillToAddressAK",
                "GeographyKey": "BillToAddressKey"
            }
        )
    if dim_alt_key == 'ShipToAddressAK':
        # temporarily rename GeographyAK into ShipToAddressAK in dim
        dim_df = dim_df.withColumnsRenamed(
            {
                "GeographyAK": "ShipToAddressAK",
                "GeographyKey": "ShipToAddressKey"
            }
        )
    # create alias
    fact_df = fact_df.alias('fact')
    dim_df = dim_df.alias('dim')
   
    # Join fact_df with dim_df on the alternative key
    joined_df = fact_df.join(dim_df.select(dim_key, dim_alt_key),
                             on=fact_df[dim_alt_key] == dim_df[dim_alt_key],
                             how="left")                             
    print('joined_df schema')
    joined_df.printSchema()


    # there are 2 cases in joined_df:
    # - the first one, all dimension alternative keys in fact are found in the dimension
    # - the second one, at least one dimension alternative key in fact is not found in the dimension, meaning Early Arriving Fact/ Late Arriving Dimension
    # the first one is a normal process and no need more process 
    # the second one needs more process that apply 'inferred member' method

    # check if having at least one dimension alternative key in joined_df is Null
    # if there is a null, then save which are inferred members
    if dim_key == 'CustomerKey': # process only for DimCustomer
        df_null_alt_key = joined_df.filter(dim_df[dim_alt_key].isNull())
        df_null_alt_key.show(10)
        print('Before:')
        count_null_alt = df_null_alt_key.count()
        print('count_null_alt:', count_null_alt)
        print('#'*30)
        if count_null_alt > 0:
            # need more process that apply 'inferred member' method
            # save which are inferred members
            inferred_members = df_null_alt_key.select(f'fact.{dim_alt_key}')
            inferred_members.write.mode("overwrite").parquet(INFERRED_MEMBER_PATH)
            inferred_members.show()
            print('Above is a list of alternative keys in fact that are not found in the dimension:')

            # drop dim_alt_key that joined from dim table in df_null_alt_key and joined_df (we have already used them to lookup where dim_key are null)
            # just keep dim_alt_key of original fact (for create new dim key task, sorted by dim_alt_key)
            df_null_alt_key = df_null_alt_key.drop(F.col(f'dim.{dim_alt_key}'))
            joined_df = joined_df.drop(F.col(f'dim.{dim_alt_key}'))

            joined_df.printSchema()
            print('#'*30)
            # get rid of df_null_alt_key from joined_df
            df_subtract = joined_df.subtract(df_null_alt_key)
            # initial new dim keys
            df_new_dim_key = _create_new_dimension_with_new_key(dimension_table=df_null_alt_key, destination_name='DimCustomer', alternative_key=dim_alt_key)
            # we need append alias name to access dim.CustomerKey in df_null_alt_key
            df_new_dim_key.show(10)
            print('After:')
            # the above function will assign new CustomerKey value respectively
            # Replace df_null_alt_key after assigning new dim key values
            joined_df = df_subtract.unionByName(df_new_dim_key)

    # drop alternative key
    joined_df = joined_df.drop(dim_alt_key)
    return joined_df # normal process when all dimension alternative keys in fact are found in the dimension



def get_new_sales(stg: DataFrame, des: DataFrame, key: str) -> DataFrame:
    # Logging
    print('#'*30)
    print('Staging Table')
    stg.show(10)
    print('Destination Table:')
    des.show(10)
    print(os.getcwd())


    # Return new data from staging table comparing to current fact table
    
    df_new = stg.join(des, on=key, how="left_anti")
    print("Neu staging dataframe joined with destination by left_anti...")
    df_new.show(10)
    df_new.printSchema()
    df_new_count = df_new.count()
    print("Count rows:", df_new_count)

    return df_new, df_new_count

if __name__ == '__main__':
    main()

