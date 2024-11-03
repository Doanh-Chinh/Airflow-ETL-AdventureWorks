
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql import types as T
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.exceptions import AirflowSkipException
from pyspark.sql.types import StructType

import os
from config_services import ServiceConfig, get_default_SparkConf, SparkSchema
from pyspark.sql.functions import current_timezone


def _create_spark_session():
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def _get_dimension_key(alternative_key: str) -> str:
    return alternative_key[:-2] + 'Key' # replace 'AK' by 'Key' that depends on your Dimension design


def _create_new_dimension_with_new_key(dimension_table: DataFrame, destination_name: str, alternative_key: str, alias=None) -> DataFrame:
    spark = SparkSession.getActiveSession()
    dimension_key = _get_dimension_key(alternative_key=alternative_key)
    # Retrieve the current maximum Dimension Key 
    print('Create Dimension Key for: ', dimension_key)
    current_max_key = spark.sql(f"SELECT COALESCE(MAX({dimension_key}), 0) AS max_key FROM {destination_name}").collect()[0]["max_key"]
    print('current max key:', current_max_key)
    print('Alias:', alias)
    if alias is not None:
        print('#'*30)
        print('Alias table:', alias)
        alternative_key = f'{alias}.{alternative_key}'
    # Define a window to calculate the new Key, starting from the max value + 1
    window_spec = Window.orderBy(alternative_key)
    df_new = dimension_table.withColumn(dimension_key, F.row_number().over(window_spec) + current_max_key)
    return df_new


def lookup_and_insert(df_staging_path: str, df_destination_name: str, alternative_key: str, insert_flag=True, update_flag=False) -> None:
    """
    Perform lookup and insert for unmatched alternative_key records.
    
    Args:
        df_staging_path (str): The path to the source DataFrame written by Spark.
        df_destination_name (str): The name of the destination Hive table.
        alternative_key (str): The alternative key used for lookup.
        insert_flag (bool): Flag to control whether to insert new records.
        update_flag (bool): Flag to control whether to update existing records.
    """
    # Create SparkSession
    spark = _create_spark_session()

    # logging
    print('#'*30)
    print(df_staging_path)
    print(df_destination_name)
    print(alternative_key)
    print(os.getcwd())
    # Read the source DataFrame written by the Spark job
    df_staging = spark.read.parquet(df_staging_path)

    # Read the destination table into a DataFrame
    df_destination = spark.table(df_destination_name)

    print('Staging Table')
    df_staging.show(10)
    print('Destination Table:')
    df_destination.show(10)

    # Insert new records if insert_flag is True, insert if there is not existing a new alternative key in destination before
    if insert_flag:
        # Perform the lookup and insert
        df_new = df_staging.join(df_destination, on=alternative_key, how="left_anti")
        df_new.printSchema()
        print("Neu dataframe joined with destination by left_anti...")
        df_new.show(10)
        df_new_count = df_new.count()
        print("Count rows:", df_new_count)
        if df_new_count > 0:
            print('Inserting new records...')
            print('*'*30)
            df_new = _create_new_dimension_with_new_key(dimension_table=df_new, destination_name=df_destination_name, alternative_key=alternative_key)

            print("A dataframe with new Keys are inserted")
            df_new.show(10)
            df_new.write \
                .mode("append") \
                .format("hive") \
                .saveAsTable(df_destination_name)
            print(f"{df_new_count} new records inserted into {df_destination_name}")
        else:
            print("No new records found for insertion.")


    # Update records if update_flag is True
    if update_flag:
        if df_destination_name in ['DimCustomer', 'DimEmployee']:
            # Step 1: Create condition to identify rows needed to update
            condition = (
                (df_destination['EmailAddress'] != df_staging['EmailAddress']) &  # EmailAddress is different and ExpirationDate is null in destination
                df_destination['ExpirationDate'].isNull()
            )

            # Create alias
            print("List of cols name:", df_destination.columns)
            df_destination = df_destination.alias("dest")
            df_staging = df_staging.alias("stg")

            # Initial updated inferred dataframe
            updated_inferred = spark.createDataFrame([], schema=StructType())
            print('#'*30)
            # Special condition for DimCustomer in order to update Inferred Members with flag column inferred = 1
            if df_destination_name == 'DimCustomer':
                inferred_condition = df_destination['Inferred'] == 1
                stg_cols = [f"stg.{col}" for col in df_staging.columns if col != 'CustomerKey']
                updated_inferred = df_destination.join(df_staging, on=alternative_key, how='inner') \
                    .filter(inferred_condition) \
                    .select('dest.CustomerKey', *stg_cols)
                if updated_inferred.count() > 0:
                    print('#'*30)
                    print('updated inferred dataframe:')
                    updated_inferred.show(10)


            # Step 2: Identify records in `df_destination` where alternative_key exists in both tables (df_staging and df_destination),
            #         EmailAddress has changed, and ExpirationDate is NULL. Set ExpirationDate to current timestamp.
            updated_expiration = df_destination.join(df_staging, on=alternative_key, how='inner') \
                .filter(condition) \
                .select([f"dest.{col}" for col in df_destination.columns]) \
                .withColumn("ExpirationDate", F.current_timestamp())
            """
            Because the codition in updated_expiration (existing same alternative key but difference EmailAddress) 
            include the condition in updated_inferred when the EmailAddress of staging table differ null. 
            Further more, we do not wanted insert new difference EmailAddress records
            into destination for updated_inferred (this should use for updated_expiration only).
            So that, we need to exclude updated_inferred from updated_expiration and then process different ways for them.
            """
            if not updated_inferred.rdd.isEmpty():
                # Exclude updated_inferred from updated_expiration
                updated_expiration = updated_expiration.join(updated_inferred, on=alternative_key, how='left_anti')

            # Count update rows
            updated_expiration_count = updated_expiration.count()
            updated_inferred_count = updated_inferred.count()

            if updated_expiration_count > 0 or updated_inferred_count > 0:
                df_combined = spark.createDataFrame([], schema=StructType())
                if updated_expiration_count > 0:
                    print("Dataframe was updated...")
                    print('df_to_update')
                    updated_expiration.show(10)
                    # Step 3: For changed alternative_key entries, insert a new record with a new dimension_key,
                    #         the updated EmailAddress from `df_staging`, and ExpirationDate as NULL.
                    new_records = df_staging.join(
                            df_destination, on=alternative_key, how="inner"
                        ).filter(
                            (df_staging["EmailAddress"] != df_destination["EmailAddress"])  # EmailAddress has changed 
                        ).select(
                            [f"stg.{col}" for col in df_staging.columns]
                        ).withColumn(
                            "LoadDate", F.current_timestamp()  # Set LoadDate to the current timestamp
                        ).withColumn(
                            "ExpirationDate", F.lit(None).cast("timestamp")  # Set ExpirationDate to NULL
                        )
                    new_records = _create_new_dimension_with_new_key(dimension_table=new_records, destination_name=df_destination_name, alternative_key=alternative_key)
                    print('df_to_insert')
                    new_records.show(10)

                    # Step 4: Combine records from updated_expiration, new_records, and the unaffected rows in df_destination
                    # Filter out rows that were updated in `updated_expiration` from `df_destination` to avoid duplication.
                    unaffected_records = df_destination.join(
                        updated_expiration, on=alternative_key, how="left_anti"  # Get rid of alternative_key records existing in updated_expiration from df_destination
                    )
                    # df_destination when join left_anti with updated_expiration will include inferred records if have
                    # Final combined DataFrame
                    df_combined = unaffected_records.unionByName(updated_expiration).unionByName(new_records)
                
                if updated_inferred_count > 0: 
                    print('Inferred Members are Found!')
                    if df_combined.rdd.isEmpty():
                        # Get rid of CustomerKey records existing in updated_inferred from df_destination 
                        unaffected_records = df_destination.join(updated_inferred, on='CustomerKey', how="left_anti")  
                        # return the df_destination where df_destination.CustomerKey exist in updated_inferred are removed
                    else:
                        # Get rid of CustomerKey records existing in updated_inferred from df_combined (after update and insert new)
                        unaffected_records = df_combined.join(updated_inferred, on='CustomerKey', how="left_anti")
                    
                    # Combining the unaffected_records and updated_inferred
                    df_combined = unaffected_records.unionByName(updated_inferred)

                # Step 5: Write the combined data back to the destination table
                print('#'*30)
                print('destination before updating')
                df_destination.show()
                print('destination after updating')
                df_combined.show()
                df_combined.write.parquet(df_destination_name)
                df_combined = spark.read.parquet(df_destination_name)  # Solving error: Can't overwrite the target that is also being read from.
                df_combined.write.mode("overwrite").format('hive').saveAsTable(df_destination_name)
                print(f"{updated_expiration_count} records updated with ExpirationDate.")
                print(f"{updated_inferred_count} records updated with Inferred Member.")

            else:
                print("No records found for update.")
        
        else:
            print("There is no process for update_flag=True")

def get_modified_date(**context):
    # SparkSession with Hive support
    spark = _create_spark_session()
    # spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

    print('Current timezone')
    print(spark.range(1).select(current_timezone()).show())
    print('#'*30)

    # Execute the SQL query
    df = spark.sql("""
        SELECT 
            COALESCE(LastSuccessfulModifiedDate, '1900-01-01 00:00:00') AS LastSuccessfulModifiedDate,
            date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS CurrentDate
        FROM Meta.DataFlow
        WHERE DataFlowName = 'DimCustomer'
    """)
    # COALESCE checks each argument in the order provided.
    # It returns the first non-null value it encounters.
    # If all arguments are null, it returns null.

    # Show the result (for debugging purposes)
    df.show()
    print('Last:', df.select('LastSuccessfulModifiedDate').first()['LastSuccessfulModifiedDate'])
    print('Current:', df.select('CurrentDate').first()['CurrentDate'])

    # Collect the result
    result = df.collect()[0].asDict()  # Convert the first row to a dictionary
    print('result:', result)
    # Push to XCom
    context['ti'].xcom_push(key='LastSuccessfulModifiedDate', value=result['LastSuccessfulModifiedDate'])
    context['ti'].xcom_push(key='CurrentDate', value=result['CurrentDate'])

    return result  # Return for logging or debugging if needed


def update_last_modified_date(dimension_name:str, ti=None):
    """
    Updates LastSuccessfulModifiedDate in Meta.DataFlow for the dimension.
    Pulls CurrentDate from XCom.
    """
    # Step 1: Pull CurrentDate from XCom
    current_date = ti.xcom_pull(task_ids='etl_dim_customer.get_modified_date', key='CurrentDate')
    
    if not current_date:
        raise ValueError("No CurrentDate found in XCom. Make sure `etl_dim_customer.get_modified_date` ran successfully.")

    # Step 2: Set up Spark session with Hive support
    spark = _create_spark_session()

    # spark.sql(f"""
    #     UPDATE Meta.DataFlow
    #     SET LastSuccessfulModifiedDate = '{current_date}'
    #     WHERE DataFlowName = '{dimension_name}'
    # """)  # UPDATE TABLE is not supported temporarily in Spark 3.5, but work in Spark 3.4. Reffering to https://github.com/apache/iceberg/issues/9960

    # Step 3: Replace old record by get rid of the old one and union with the new updated one
    # Read the Meta.Dataflow table
    df_cur_data_flow = spark.table("Meta.DataFlow")

    # Filter out the remaining records by get rid of the old one
    df_remain = df_cur_data_flow.filter(df_cur_data_flow['DataFlowName'] != dimension_name)
    print('#'*30)
    print('df_remain:')
    df_remain.show()
    # Create a DataFrame with the updated record
    df_update = spark.createDataFrame([(dimension_name, current_date)], schema=["DataFlowName", "LastSuccessfulModifiedDate"])  # need to corrected position with the current schema since Union resolves columns by position (not by name)

    # Union and save temporarily
    df_union = df_remain.union(df_update)  # append new update record
    print('#'*30)
    print('df_union:')
    df_union.show()
    df_union.write.mode("overwrite").parquet("dataflow_table")

    # Read df_union. Reffering to https://stackoverflow.com/questions/38746773/read-from-a-hive-table-and-write-back-to-it-using-spark-sql
    df_union = spark.read.parquet("dataflow_table")

    # Overwrite Meta.Dataflow table
    df_union.write.mode("overwrite").format('hive').saveAsTable("Meta.DataFlow")

    print(f"LastSuccessfulModifiedDate updated to {current_date} for {dimension_name} in Meta.DataFlow.")

def _is_existed_path(path: str) -> bool:
    # Hook to HDFS through Airflow WebHDFSHook
    webhdfs_hook = WebHDFSHook()
    # check if exist path
    return webhdfs_hook.check_for_path(path)


def check_skip_create_new_staging_source(staging_path: str, staging_name: str, look_insert_task: str, load_stg_task: str, **context) -> str:
    """
    This function "branch" a path after the execution of this task.
    Return the next task_id should be execute and skip some tasks if need.

    Parameter:
    - staging_path: a path to staging parquet file
    - staging_name: name of staging file
    - look_insert_task: the task id of lookup and insert 
    - load_stg_task: the task id of load_stg_dim

    Return:
    - look_insert_task: if there is a exist staging file and No need to update
    - load_stg_task: if there is not staging file found in the staging_path or there is a exist one but need to update 
    """

    # check exist staging file
    is_existed = _is_existed_path(path=staging_path) # true if existing file
    # stage - source mapping
    stage_source_maps = {
        'STG_DIM_PROMOTION': [
            {'data': 'SpecialOffer'},
            {'task_group_id': 'etl_dim_promotion'}
        ],
        'STG_DIM_CUSTOMER': [
            {'data': ['Customer', 'Person', 'EmailAddress']},
            {'task_group_id': 'etl_dim_customer'}                
        ],
        'STG_DIM_EMPLOYEE': [
            {'data': ['Employee', 'Person', 'EmailAddress']},
            {'task_group_id': 'etl_dim_employee'}
        ],
        'STG_DIM_GEOGRAPHY': [
            {'data': ['Address', 'StateProvince', 'CountryRegion']},
            {'task_group_id': 'etl_dim_geography'}
        ],
        'STG_DIM_SALES_TERRITORY': [
            {'data': ['SalesTerritory', 'CountryRegion']},                 
            {'task_group_id': 'etl_dim_salesterritory'}
        ],
        'STG_DIM_PRODUCT': [
            {'data': ['Product', 'ProductSubcategory', 'ProductCategory', 'ProductModel']},
            {'task_group_id': 'etl_dim_product'}
        ],
        'STG_FACT_SALES': [
            {'data': ['Product', 'SalesOrderHeader', 'SalesOrderDetail']},
            {'task_group_id': 'load_stg_sales'}
        ]
    }

    lst_src_files = stage_source_maps[staging_name][0]['data']
    task_group_id = stage_source_maps[staging_name][1]['task_group_id']
    print('lst_src_files:', lst_src_files)
    print('task_group_id:', task_group_id)
    print('#'*30)

    if is_existed:
        # check last modified date to check if need save a new staging file, overwrite old one.
        is_updated = False  # set defaut is_updated is false, meaning No need to update 

        # if at least one data source file of staging file found as new, then the staging file need to update.
        # Otherwise, the staging file does not need to update.       

        # loop file in list of source files to check if there is at least a new source file 
        for file in lst_src_files:
            value_pull = context['ti'].xcom_pull(key=file, task_ids=f'upload_from_local.{file}')
            if value_pull == 'upload':
                is_updated = True
                break
                
        if is_updated:
            # there is a exist one but STG (Staging) need to update 
            return f'{task_group_id}.{load_stg_task}'
        else:
            # there is a exist one and No need to update 
            print(f"Branch decision: {task_group_id}.{load_stg_task} will skip.")
            return f'{task_group_id}.{look_insert_task}' # skip load STG task
    else:
        # there is not staging file found in the staging_path
        return f'{task_group_id}.{load_stg_task}' # no skip and need to update staging file


def insert_inferred_member(dim_name:str, inferred_member_path: str): 
    """
    Load inferred member from inferred_member_path then insert them into dim_name
    
    Parameters:
    - dim_name: the dimension name should be inserted inferred members.
    - inferred_member_path: the path to list of inferred members.
    
    """

    if not _is_existed_path(inferred_member_path):
        # skip this task
        raise AirflowSkipException("There is no inferred member path. Skip this task!!")

    spark = _create_spark_session()
    # read list of inferred members
    df_inferred = spark.read.parquet(inferred_member_path)
    print('df_inferred shows')
    df_inferred.show()
    # dim alternative key
    dim_alt_key = df_inferred.columns[0]

    # insert inferred members into respective dimension
    # Define the schema for the inferred records, matching DimCustomer structure
    # the default values for each column
    default_values = {
        "CustomerKey": F.lit(None).cast(T.IntegerType()),  # Unique key for each inferred row
        "Title": F.lit(None).cast(T.StringType()),
        "FirstName": F.lit(None).cast(T.StringType()),
        "MiddleName": F.lit(None).cast(T.StringType()),
        "LastName": F.lit(None).cast(T.StringType()),
        "Suffix": F.lit(None).cast(T.StringType()),
        "EmailAddress": F.lit(None).cast(T.StringType()),
        "ExpirationDate": F.lit(None).cast(T.TimestampType()),
        "LoadDate": F.lit(F.current_timestamp()).cast(T.TimestampType()),
        "inferred": F.lit(1).cast(T.ByteType())  # Flag as inferred member
    }
    
    # Add all default columns using withColumns
    df_inferred_with_defaults = df_inferred.withColumns(default_values)
    df_inferred_with_defaults = _create_new_dimension_with_new_key(dimension_table=df_inferred_with_defaults, destination_name=dim_name, alternative_key=dim_alt_key)
    df_inferred_with_defaults.printSchema()
    df_inferred_with_defaults.show()
    
    # Insert records into DimCustomer
    df_inferred_with_defaults.write \
        .mode("append") \
        .format('hive') \
        .saveAsTable(dim_name)
    print('*'*30)
    print('Success insert inferred members into dim')
    
if __name__ == "__main__":
    lookup_and_insert()