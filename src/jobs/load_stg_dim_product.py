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

STG_DIM_PRODUCT = os.getenv('STG_DIM_PRODUCT')

def main() -> None:
    """Load product dimension table"""
    # Get configs
    dir_path = "/data_lake"
    
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()

    df_src_product = spark.read.option("delimiter", "\t").csv(dir_path + "/Product.csv", schema = SCHEMAS.src_product)  # the option is need to avoding None return
    df_src_subCategory = spark.read.option("delimiter", "\t").csv(dir_path + "/ProductSubcategory.csv", schema = SCHEMAS.src_productSubCategory)
    df_src_category = spark.read.option("delimiter", "\t").csv(dir_path + "/ProductCategory.csv", schema = SCHEMAS.src_productCategory)
    df_src_productModel = spark.read.option("delimiter", "\t").csv(dir_path + "/ProductModel.csv", schema = SCHEMAS.src_productModel)


    df_src_product.show(5)
    df_src_subCategory.show(5)
    df_src_category.show(5)
    df_src_productModel.show(5)

    # extract transform product dimension
    df_dim_product = preprocess_dim_product(df_product=df_src_product, df_productSubCate=df_src_subCategory, df_productCate=df_src_category, df_productModel=df_src_productModel)

    # Add dim_key column and rearrange columns order
    print("the dimension key will be created in post process then")
    df_dim_product = df_dim_product \
        .withColumn(
            "ProductKey", 
            F.lit(None).cast("int")  # the dimension key will be created in post process then
        )
    # save as staging parquet
    df_dim_product.show(10)   # for logging purposes
    df_dim_product.printSchema()

    print(STG_DIM_PRODUCT)
    df_dim_product.write.parquet(STG_DIM_PRODUCT, mode="overwrite")
    print('load_dim_product_success')    
    print('*'*30)


def preprocess_dim_product(df_product: DataFrame, df_productSubCate: DataFrame, df_productCate: DataFrame, df_productModel: DataFrame) -> DataFrame:
    spark = SparkSession.getActiveSession()
    # Registering DataFrame as temporary views
    df_product.createOrReplaceTempView("Product")
    df_productSubCate.createOrReplaceTempView("ProductSubcategory")
    df_productCate.createOrReplaceTempView("ProductCategory")
    df_productModel.createOrReplaceTempView("ProductModel")
    
    # Perform SQL query
    sql_query = """
        SELECT P.ProductID as ProductAK
            ,P.ProductNumber
            ,P.Name as ProductName
            ,PSC.Name as ProductSubCategoryName
            ,PC.Name as ProductCategoryName
            ,P.FinishedGoodsFlag
            ,P.Color
            ,P.StandardCost
            ,P.ListPrice
            ,P.Size
            ,P.ProductLine
            ,P.Class
            ,P.Style
            ,M.Name as ProductModelName
            ,P.SellStartDate
            ,P.SellEndDate
        FROM Product P 
        LEFT JOIN ProductSubcategory PSC
            ON P.ProductSubcategoryID = PSC.ProductSubcategoryID
        LEFT JOIN ProductCategory PC
            ON PSC.ProductCategoryID = PC.ProductCategoryID
        LEFT JOIN ProductModel M
            ON P.ProductModelID = M.ProductModelID
    """
    print(sql_query)
    # Execute the query
    sqlDF = spark.sql(sql_query)

    sqlDF.show()
    print("#"*30)
    print("Distinct values of Product.ProductModelName column before:")
    sqlDF.select("ProductModelName").distinct().show(truncate=False)

    sqlDF = field_vals_to_nulls(
        sqlDF,
        {
            "ProductModelName": ["NULL"],
        }
    )
    print("Distinct values of Product.ProductModelName column after:")
    print("#"*30)
    sqlDF.select("ProductModelName").distinct().show(truncate=False)
    return sqlDF


def field_vals_to_nulls(
    df: DataFrame, 
    col_map: dict[str, List]
) -> DataFrame:
    """In case dataframe has different words that represent NULL, reset those 
    words to empty string."""
    # If field value in word list then set to empty value, else retain value
    for col, words in col_map.items():
        bool_expr = ~(F.col(col) == F.col(col)) # initially False
        for w in words:
            bool_expr |= (F.col(col) == w) # only becomes True when word found
            
        df = df.withColumn(
            col, 
            F.when(~(bool_expr), F.col(col)).otherwise("") # empty string
        )

    return df


if __name__ == "__main__":
    main() 
    