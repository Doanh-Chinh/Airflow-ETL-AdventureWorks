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

STG_DIM_EMPLOYEE = os.getenv("STG_DIM_EMPLOYEE")

def main() -> None:
    """Load employee dimension table"""
    # Get configs
    dir_path = "/data_lake"
    
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()

    df_src_employee = spark.read.option("delimiter", "\t").csv(dir_path + "/Employee.csv", schema = SCHEMAS.src_employee)  # the option is need to avoding None return
    df_src_person = spark.read.option("delimiter", "\t").csv(dir_path + "/Person.csv", schema = SCHEMAS.src_person)
    df_src_emailAddress = spark.read.option("delimiter", "\t").csv(dir_path + "/EmailAddress.csv", schema = SCHEMAS.src_emailAddress)
    df_src_employee.show(5)
    df_src_person.show(5)
    df_src_emailAddress.show(5)

    # extract transform employee dimension
    df_dim_employee = preprocess_dim_employee(df_employee=df_src_employee, df_person=df_src_person, df_emailAddress=df_src_emailAddress)

    # Add dim_key column and rearrange columns order
    df_dim_employee = df_dim_employee \
        .withColumn(
            "EmployeeKey", 
            F.lit(None).cast("int")  # the dimension key will be created in post process then
        )
    # save as staging parquet
    df_dim_employee.show(10)   # for logging purposes
    df_dim_employee.printSchema()

    df_dim_employee.write.parquet(STG_DIM_EMPLOYEE, mode="overwrite")
    print('load_dim_employee_success')
    print(STG_DIM_EMPLOYEE)
    print('*'*30)


def preprocess_dim_employee(df_employee: DataFrame, df_person: DataFrame, df_emailAddress: DataFrame) -> DataFrame:
    spark = SparkSession.getActiveSession()
    # Registering DataFrame as temporary views
    df_employee.createOrReplaceTempView("Employee")
    df_person.createOrReplaceTempView("Person")
    df_emailAddress.createOrReplaceTempView("EmailAddress")
    
    # Perform SQL query
    sql_query = """
        SELECT e.BusinessEntityID as EmployeeAK,
            p.Title,
            p.FirstName,
            p.LastName,
            p.MiddleName,
            p.Suffix,
            ea.EmailAddress,
            e.JobTitle,
            e.CurrentFlag
        FROM Employee e
        LEFT JOIN Person p
            ON e.BusinessEntityID = p.BusinessEntityID
        LEFT JOIN EmailAddress ea
            ON e.BusinessEntityID = ea.BusinessEntityID
    """
    print(sql_query)
    # Execute the query
    sqlDF = spark.sql(sql_query)

    # Add a column "LoadDate" with the current timestamp and ExpirationDate are set to NULL
    sqlDF = sqlDF.withColumn("LoadDate", F.current_timestamp()) \
        .withColumn("ExpirationDate", F.lit(None).cast("timestamp"))

    sqlDF.show()
    return sqlDF

if __name__ == "__main__":
    main() 
    