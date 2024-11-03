from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import *
import pyspark.sql.functions as F
import argparse
from airflow.exceptions import AirflowSkipException

from typing import List
from datetime import datetime
import hdfs
import json
import os
print('='*30)
print(os.getcwd())

from config_services import ServiceConfig, get_default_SparkConf, SparkSchema
SCHEMAS = SparkSchema()
WEBHDFS_URI = ServiceConfig("webhdfs").uri

STG_DIM_PROMOTION = os.getenv("STG_DIM_PROMOTION")

def main() -> None:
    """Load promotion dimension table"""
    dir_path = "/data_lake"
    
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()

    # Get promotion data from data lake and preprocess
    # df_dim_promotion = preprocess_promotion(
    #     spark.read.csv(
    #         dir_path + "/SpecialOffer.csv", 
    #         schema = SCHEMAS.src_promotion
    #     )
    # )
    df_dim_promotion = preprocess_promotion(
        spark.read.option("delimiter", "\t").csv(dir_path + "/SpecialOffer.csv", schema = SCHEMAS.src_promotion)  # the option is need to avoding None return
    )
    df_dim_promotion.show(5)

    print("the dimension key will be created in post process then")
    df_dim_promotion = df_dim_promotion \
        .withColumn(
            "PromotionKey", 
            F.lit(None).cast("int")  # the dimension key will be created in post process then
        )     
   
   
    # save as staging parquet
    df_dim_promotion.show(10)   # for logging purposes
    df_dim_promotion.printSchema()

    df_dim_promotion.write.parquet(STG_DIM_PROMOTION, mode="overwrite")
    print('load_dim_promotion_success')
    print(STG_DIM_PROMOTION)
    print('*'*30)


def preprocess_promotion(df: DataFrame) -> DataFrame:
    """Select & rename columns. Drop duplicate rows."""
    return df.select(
            "SpecialOfferID", 
            "Description", 
            "DiscountPct", 
            "Type",
            "Category",
            "StartDate",
            "EndDate",
            "MinQty",
            "MaxQty"

        ) \
        .withColumnsRenamed(
            {
        # code here
            "SpecialOfferID": "PromotionAK",
            "Description": "PromotionName",
            "DiscountPct": "DiscountPercentage",
            "Type": "PromotionType",
            "Category": "PromotionCategory",
            "StartDate": "PromotionStartDate",
            "EndDate": "PromotionEndDate",
            "MinQty": "MinQuantity",
            "MaxQty": "MaxQuantity"
            }
        ) \
        .drop_duplicates()


       


if __name__ == "__main__":
    main() # have to call it to run
    