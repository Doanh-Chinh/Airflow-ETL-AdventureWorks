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

STG_DIM_CUSTOMER = os.getenv("STG_DIM_CUSTOMER")

def main(last_successful_date, current_date) -> None:
    """Load customer dimension table"""
    # Get configs
    dir_path = "/data_lake"
    
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()


    df_src_customer = spark.read.option("delimiter", "\t").csv(dir_path + "/Customer.csv", schema = SCHEMAS.src_customer)  # the option is need to avoding None return
    df_src_person = spark.read.option("delimiter", "\t").csv(dir_path + "/Person.csv", schema = SCHEMAS.src_person)
    df_src_emailAddress = spark.read.option("delimiter", "\t").csv(dir_path + "/EmailAddress.csv", schema = SCHEMAS.src_emailAddress)
    df_src_customer.show(5)
    df_src_person.show(5)
    df_src_emailAddress.show(5)

    # extract transform customer dimension
    df_dim_customer = preprocess_dim_customer(df_customer=df_src_customer, df_person=df_src_person, df_emailAddress=df_src_emailAddress, last_successful_date=last_successful_date, current_date=current_date)

    # Add new default Customer Key and Inferred Member flag. 
    # InferredMember = 0: the customer record is already updated or loaded
    # InferredMember = 1: the customer record is infferred from early arriving fact and need to update this customer record in future load

    print("the dimension key will be created in post process then")
    df_dim_customer = df_dim_customer \
        .withColumns(
            {
                "CustomerKey": F.lit(None).cast("int"),  # the dimension key will be created in post process then
                "Inferred": F.lit(0).cast("tinyint")
            }
        )   
    # save as staging parquet
    df_dim_customer.show(10)   # for logging purposes
    df_dim_customer.printSchema()

    df_dim_customer.write.parquet(STG_DIM_CUSTOMER, mode="overwrite")
    print('load_dim_customer_success')
    print(STG_DIM_CUSTOMER)
    print('*'*30)


def preprocess_dim_customer(df_customer: DataFrame, df_person: DataFrame, df_emailAddress: DataFrame, last_successful_date, current_date) -> DataFrame:
    spark = SparkSession.getActiveSession()
    # Registering DataFrame as temporary views
    df_customer.createOrReplaceTempView("Customer")
    df_person.createOrReplaceTempView("Person")
    df_emailAddress.createOrReplaceTempView("EmailAddress")

    # log.info("Print current date and last successful date...")
    print("Print current date and last successful date...")
    print("Current date:", current_date)
    print("Last successful date:", last_successful_date)
    


    # Perform SQL query using f-strings to insert the parameters
    sql_query = f"""
        SELECT c.CustomerID AS CustomerAK,
               p.Title,
               p.FirstName,
               p.LastName,
               p.MiddleName,
               p.Suffix,
               em.EmailAddress
        FROM Customer c
        JOIN Person p 
            ON c.PersonID = p.BusinessEntityID
        LEFT JOIN EmailAddress em 
            ON p.BusinessEntityID = em.BusinessEntityID
        WHERE (em.ModifiedDate > '{last_successful_date}' 
               OR p.ModifiedDate > '{last_successful_date}')
          AND (em.ModifiedDate <= '{current_date}' 
               OR p.ModifiedDate <= '{current_date}')
    """
    print(sql_query)
    # Execute the query
    sqlDF = spark.sql(sql_query)
    # Add a column "LoadDate" with the current timestamp
    

    sqlDF = sqlDF.withColumn("LoadDate", F.current_timestamp()) \
        .withColumn("ExpirationDate", F.lit(None).cast("timestamp"))

    print(F.current_timestamp())
    sqlDF.show()
    print("Num rows:", sqlDF.count())
    print('#'*30)

    
    return sqlDF
     


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--last_success_date", help="Last successful modified date pulled from XCom")
    parser.add_argument("--current_date", help="Current date pulled from XCom")
    args = parser.parse_args()
    print('Current date:', args.current_date)
    main(last_successful_date=args.last_success_date, current_date=args.current_date) 
    