from airflow import DAG

from airflow.decorators.task_group import task_group
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit \
    import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

import pendulum
import os
import time

from utils import lookup_and_insert, get_modified_date, update_last_modified_date, check_skip_create_new_staging_source, insert_inferred_member

from datetime import datetime
import pytz
# Specify the timezone
tz = pytz.timezone("Asia/Ho_Chi_Minh")


# Paths
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DIR_DAGS = f"{AIRFLOW_HOME}/dags"
DIR_JOBS = f"{AIRFLOW_HOME}/jobs"
STG_DIM_PROMOTION = os.getenv("STG_DIM_PROMOTION")
STG_DIM_CUSTOMER = os.getenv("STG_DIM_CUSTOMER")
STG_DIM_EMPLOYEE = os.getenv("STG_DIM_EMPLOYEE")
STG_DIM_GEOGRAPHY = os.getenv("STG_DIM_GEOGRAPHY")
STG_DIM_SALES_TERRITORY = os.getenv("STG_DIM_SALES_TERRITORY")
STG_DIM_PRODUCT = os.getenv("STG_DIM_PRODUCT")
STG_FACT_SALES = os.getenv("STG_FACT_SALES")
INFERRED_MEMBER_PATH = os.getenv("INFERRED_MEMBER_PATH")



DIR_CONFIG = f"{AIRFLOW_HOME}/config"

# DAG's default arguments
default_args = {
    "start_date": pendulum.datetime(2024, 11, 3),
    "schedule": None,
    "retries": 0,
}


# 

with DAG(
    dag_id = "etl_adventureworks",
    description = "ETL pipeline for oltp adventureworks",
    default_args = default_args
) as dag:
    # Hook to HDFS through Airflow WebHDFSHook
    webhdfs_hook = WebHDFSHook()

    # Default args for SparkSubmitOperators
    default_py_files = f"{DIR_CONFIG}/config_services.py"


    """Upload files from local to data lake"""
    @task_group(
        group_id = "upload_from_local"
    )
    def upload_local():
        # Add skip functionality for DAG logic and monitoring
        # Skipped -> File already exists
        def _upload(local_path: str, hdfs_path: str, skips: bool, **context) -> None:
            # if skips:
            #     raise AirflowSkipException  # need to enhance this process in case the data source has updated, then 
                # the data lake folder in hadoop need to update following instead of skip
                # then we can process incremental load in flowing step in DAG setup
            
            # extract file name from hdfs_path as file key for xcom_push
            file_key = hdfs_path.split('/')[-1].replace('.csv', '')
            print('#'*30)
            print('file key:', file_key)

            # Updated _upload function to handle file updates based on modification times
            if skips:
                local_mod_time = os.path.getmtime(local_path)  # Local file modification time
                local_mod_time = datetime.fromtimestamp(local_mod_time, tz=tz)

                hdfs_client = webhdfs_hook.get_conn()
                # Get file status response
                response = hdfs_client._get_file_status(hdfs_path)
                
                # Parse the response as JSON
                hdfs_status = response.json()  # Convert response content to JSON

                # Access the modification time in milliseconds
                modification_time = hdfs_status['FileStatus']['modificationTime']
                
                # Convert from milliseconds to a Python datetime object
                hdfs_mod_time = datetime.fromtimestamp(modification_time / 1000, tz=tz)
                print('#'*30)
                print('HDFS Response:', hdfs_status)
                print('last modified date in hdfs:', hdfs_mod_time)
                print('last modified date in local:', local_mod_time)

                # Check if local file is more recent than the one in data lake
                if local_mod_time <= hdfs_mod_time:
                    print(f"File {local_path} has not changed. Skipping upload.")
                    context['ti'].xcom_push(key=file_key, value='skip')
                    raise AirflowSkipException  # Skip upload as data lake version is up to date
                
                print(f"File {local_path} has changed. Updating data lake file.")


            # Upload the file to the data lake    
            webhdfs_hook.load_file(local_path, hdfs_path)
            context['ti'].xcom_push(key=file_key, value='upload')

        # Internal helper to create a file-task id template
        def _create_file_task_id_template() -> dict: # internal use
            res = {}
            data_files = [f for f in os.listdir('../../data') if f.endswith('.csv')]  # lst data filenames
            # mapping filenames 
            for file in data_files:
                # if file not in ['Product.csv', 'SalesOrderHeader.csv', 'SalesOrderDetail.csv', 'Customer.csv', 'Person.csv', 'EmailAddress.csv']: # 'SpecialOffer.csv', 'Product.csv', 'ProductSubcategory.csv', 'ProductCategory.csv', 'ProductModel.csv', 'SalesOrderHeader.csv', 'SalesOrderDetail.csv'
                #     continue
                res.update({file: os.path.splitext(file)[0]})  # {name with extension: name without extension}
            return res

        file_task_id_template = _create_file_task_id_template()  # mapping filenames to task id template

        for file in file_task_id_template.keys():
            local_path = f"/data/{file}"
            hdfs_path = f"/data_lake/{file}"
            
            params = {
                "local_path": local_path,
                "hdfs_path": hdfs_path,
                "skips": webhdfs_hook.check_for_path(hdfs_path)
            }
            task_id_templated = file_task_id_template[file]     

            PythonOperator(
                task_id = task_id_templated,
                python_callable = _upload, 
                op_kwargs = params,
                provide_context=True             

            )


    """Create Hive tables in data warehouse"""
    with open(f"{DIR_DAGS}/hql/create_hive_tbls.hql", "r") as script:
        create_hive_tbls = HiveOperator(
            task_id = "create_hive_tbls",
            hql = script.read(),
        )
#region
    """Transform and load dimension promotion to data warehouse"""
    with TaskGroup("etl_dim_promotion", tooltip="Tasks for etl_dim_promotion", default_args = {
            "trigger_rule": "none_failed",
            "py_files": default_py_files
        }) as etl_dim_promotion:

        check_skip_load_dim = BranchPythonOperator( # calls a Python function that evaluates a condition and returns the task ID(s) to execute next.
            task_id="check_skip_load_dim",
            python_callable=check_skip_create_new_staging_source, 
            op_kwargs={
                'staging_path': STG_DIM_PROMOTION,
                'staging_name': 'STG_DIM_PROMOTION',
                'look_insert_task': 'lookup_and_insert',
                'load_stg_task': 'stg_dim_promotion'
            },
            provide_context=True
        )

        # Run Spark job to load stg_dimpromotion
        stg_dim_promotion = SparkSubmitOperator(
            task_id="stg_dim_promotion",
            name="Load dim promotion to data warehouse",
            application=f"{DIR_JOBS}/load_stg_dim_promotion.py",
        )
        
        # Run the Python function lookup_and_insert
        lookup_and_insert_task = PythonOperator(
            task_id="lookup_and_insert",
            python_callable=lookup_and_insert, 
            op_kwargs={
                'df_staging_path': STG_DIM_PROMOTION,
                'df_destination_name': 'DimPromotion',
                'alternative_key': 'PromotionAK'
            }
        )

        # Setting dependencies
        check_skip_load_dim >> stg_dim_promotion >> lookup_and_insert_task
        

    """Transform and load dimension customer to data warehouse"""
    with TaskGroup(
        group_id = "etl_dim_customer",
        default_args = {
            "trigger_rule": "none_failed",
            "py_files": default_py_files
        }
    ) as etl_dim_customer:
        get_modified_date = PythonOperator(
            task_id="get_modified_date",
            python_callable=get_modified_date,
            provide_context=True             
        )

        check_skip_load_dim = BranchPythonOperator( # calls a Python function that evaluates a condition and returns the task ID(s) to execute next.
            task_id="check_skip_load_dim",
            python_callable=check_skip_create_new_staging_source, 
            op_kwargs={
                'staging_path': STG_DIM_CUSTOMER,
                'staging_name': 'STG_DIM_CUSTOMER',
                'look_insert_task': 'lookup_and_insert',
                'load_stg_task': 'stg_dim_customer'
            },
            provide_context=True
        )

        stg_dim_customer = SparkSubmitOperator(
            task_id = "stg_dim_customer",
            name = "Load dim customer to data warehouse",
            application = f"{DIR_JOBS}/load_stg_dim_customer.py",
            application_args=[            
            "--last_success_date", "{{ ti.xcom_pull(task_ids='etl_dim_customer.get_modified_date', key='LastSuccessfulModifiedDate') }}",  # add task group id before sub task
            "--current_date", "{{ ti.xcom_pull(task_ids='etl_dim_customer.get_modified_date', key='CurrentDate') }}"
            ]

        )

        lookup_and_insert_task = PythonOperator(
            task_id="lookup_and_insert",
            python_callable=lookup_and_insert, 
            op_kwargs={
                'df_staging_path': STG_DIM_CUSTOMER,
                'df_destination_name': 'DimCustomer',
                'alternative_key': 'CustomerAK',
                'update_flag': True
            }
        )

        update_last_modified_date = PythonOperator(
            task_id="update_last_modified_date",
            python_callable=update_last_modified_date, 
            op_kwargs={
                'dimension_name': 'DimCustomer'
            }
        )

        # Setting dependencies
        get_modified_date >> check_skip_load_dim >> stg_dim_customer >> lookup_and_insert_task >> update_last_modified_date 
        

    """Transform and load dimension employee to data warehouse"""
    with TaskGroup(
        group_id = "etl_dim_employee",
        default_args = {
            "trigger_rule": "none_failed",
            "py_files": default_py_files
        }
    ) as etl_dim_employee:
        check_skip_load_dim = BranchPythonOperator( # calls a Python function that evaluates a condition and returns the task ID(s) to execute next.
            task_id="check_skip_load_dim",
            python_callable=check_skip_create_new_staging_source, 
            op_kwargs={
                'staging_path': STG_DIM_EMPLOYEE,
                'staging_name': 'STG_DIM_EMPLOYEE',
                'look_insert_task': 'lookup_and_insert',
                'load_stg_task': 'stg_dim_employee'
            },
            provide_context=True
        )

        stg_dim_employee = SparkSubmitOperator(
            task_id = "stg_dim_employee",
            name = "Load dim employee to data warehouse",
            application = f"{DIR_JOBS}/load_stg_dim_employee.py"
        ) 

        lookup_and_insert_task = PythonOperator(
            task_id="lookup_and_insert",
            python_callable=lookup_and_insert, 
            op_kwargs={
                'df_staging_path': STG_DIM_EMPLOYEE,
                'df_destination_name': 'DimEmployee',
                'alternative_key': 'EmployeeAK',
                'update_flag': True
            }
        )
        check_skip_load_dim >> stg_dim_employee >> lookup_and_insert_task

    """Transform and load dimension geography to data warehouse"""
    with TaskGroup(
        group_id = "etl_dim_geography",
        default_args = {
            "trigger_rule": "none_failed",
            "py_files": default_py_files
        }
    ) as etl_dim_geography:
        check_skip_load_dim = BranchPythonOperator( # calls a Python function that evaluates a condition and returns the task ID(s) to execute next.
            task_id="check_skip_load_dim",
            python_callable=check_skip_create_new_staging_source, 
            op_kwargs={
                'staging_path': STG_DIM_GEOGRAPHY,
                'staging_name': 'STG_DIM_GEOGRAPHY',
                'look_insert_task': 'lookup_and_insert',
                'load_stg_task': 'stg_dim_geography'
            },
            provide_context=True
        )

        stg_dim_geography = SparkSubmitOperator(
            task_id = "stg_dim_geography",
            name = "Load dim geography to data warehouse",
            application = f"{DIR_JOBS}/load_stg_dim_geography.py"
        )

        lookup_and_insert_task = PythonOperator(
            task_id="lookup_and_insert",
            python_callable=lookup_and_insert, 
            op_kwargs={
                'df_staging_path': STG_DIM_GEOGRAPHY,
                'df_destination_name': 'DimGeography',
                'alternative_key': 'GeographyAK',
            }
        )
        check_skip_load_dim >> stg_dim_geography >> lookup_and_insert_task 
#endregion

    """Transform and load dimension salesterritory to data warehouse"""
    with TaskGroup(
        group_id = "etl_dim_salesterritory",
        default_args = {
            "trigger_rule": "none_failed",
            "py_files": default_py_files
        }
    ) as etl_dim_salesterritory:
        check_skip_load_dim = BranchPythonOperator( # calls a Python function that evaluates a condition and returns the task ID(s) to execute next.
            task_id="check_skip_load_dim",
            python_callable=check_skip_create_new_staging_source, 
            op_kwargs={
                'staging_path': STG_DIM_SALES_TERRITORY,
                'staging_name': 'STG_DIM_SALES_TERRITORY',
                'look_insert_task': 'lookup_and_insert',
                'load_stg_task': 'stg_dim_salesterritory'
            },
            provide_context=True
        )

        stg_dim_salesterritory = SparkSubmitOperator(
            task_id = "stg_dim_salesterritory",
            name = "Load dim salesterritory to data warehouse",
            application = f"{DIR_JOBS}/load_stg_dim_salesterritory.py"
        )  

        lookup_and_insert_task = PythonOperator(
            task_id="lookup_and_insert",
            python_callable=lookup_and_insert, 
            op_kwargs={
                'df_staging_path': STG_DIM_SALES_TERRITORY,
                'df_destination_name': 'DimSalesTerritory',
                'alternative_key': 'SalesTerritoryAK',
            }
        )
        check_skip_load_dim >> stg_dim_salesterritory >> lookup_and_insert_task

#region
    """Transform and load dimension product to data warehouse"""
    with TaskGroup(
        group_id = "etl_dim_product",
        default_args = {
            "trigger_rule": "none_failed",
            "py_files": default_py_files
        }
    ) as etl_dim_product:
        check_skip_load_dim = BranchPythonOperator( # calls a Python function that evaluates a condition and returns the task ID(s) to execute next.
            task_id="check_skip_load_dim",
            python_callable=check_skip_create_new_staging_source, 
            op_kwargs={
                'staging_path': STG_DIM_PRODUCT,
                'staging_name': 'STG_DIM_PRODUCT',
                'look_insert_task': 'lookup_and_insert',
                'load_stg_task': 'stg_dim_product'
            },
            provide_context=True
        )

        stg_dim_product = SparkSubmitOperator(
            task_id = "stg_dim_product",
            name = "Load dim product to data warehouse",
            application = f"{DIR_JOBS}/load_stg_dim_product.py"
        )

        lookup_and_insert_task = PythonOperator(
            task_id="lookup_and_insert",
            python_callable=lookup_and_insert, 
            op_kwargs={
                'df_staging_path': STG_DIM_PRODUCT,
                'df_destination_name': 'DimProduct',
                'alternative_key': 'ProductAK',
            }
        )   
        check_skip_load_dim >> stg_dim_product >> lookup_and_insert_task  
                      
    """Populate and load dim date to data warehouse"""
    load_dates = SparkSubmitOperator(
        task_id = "dim_dates",
        name = "Prepopulate dates dim table to data warehouse",
        application = f"{DIR_JOBS}/load_dim_dates.py",
        application_args = ["1999-12-31", "2025-01-02"],
        py_files = default_py_files,
        trigger_rule = 'none_failed'
    )

    """Transform and load stage  sales transaction into fact table before lookup dimension key in data warehouse"""
    with TaskGroup(
        group_id='load_stg_sales',
        default_args = {
            'trigger_rule': 'none_failed',
            'py_files': default_py_files
        }
    ) as load_stg_sales:
        check_skip_load_stg_sales = BranchPythonOperator( # calls a Python function that evaluates a condition and returns the task ID(s) to execute next.
        task_id="check_skip_load_stg_sales",
        python_callable=check_skip_create_new_staging_source, 
        op_kwargs={
            'staging_path': STG_FACT_SALES,
            'staging_name': 'STG_FACT_SALES',
            'look_insert_task': 'dummy_skip_task',
            'load_stg_task': 'load_stg_sales_task'
        },
        provide_context=True
        )

        load_stg_sales_task = SparkSubmitOperator(
            task_id = "load_stg_sales_task",
            name = "Transform and load  sales data to data warehouse",
            application = f"{DIR_JOBS}/load_stg_sales.py",
        )

        # Dummy task to handle skip logic
        dummy_skip_task = DummyOperator(
            task_id="dummy_skip_task"
        )
        check_skip_load_stg_sales >> load_stg_sales_task >> dummy_skip_task

    """Transform and load fact table to data warehouse"""
    with TaskGroup(
        group_id='load_fct_sales',
        default_args={
            'trigger_rule': 'none_failed',
            'py_files': default_py_files
        }
    ) as load_fct_sales:
        load_fct_sales_task = SparkSubmitOperator(
            task_id = "load_fct_sales",
            name = "Transform and load  sales data to data warehouse",
            application = f"{DIR_JOBS}/load_fct_sales.py",
        )

        insert_inferred_member_task = PythonOperator(
            task_id = "insert_inferred_member",
            python_callable = insert_inferred_member,
            op_kwargs = {
                "dim_name": "DimCustomer",
                "inferred_member_path": INFERRED_MEMBER_PATH
            }
        )

        # Delete the old inferred member file
        delete_old_inferred_member_file = BashOperator(
            task_id="delete_old_inferred_member_file",
            bash_command=(
                'curl -i -X DELETE "http://host.docker.internal:{{ var.value.webhdfs_port }}/webhdfs/v1/inferred_member/inferred_member.parquet?op=DELETE&recursive=true"'
                )
        )
        load_fct_sales_task >> insert_inferred_member_task >> delete_old_inferred_member_file

 #endregion
    """Task dependencies"""
    create_hive_tbls >> upload_local() >> [etl_dim_promotion, etl_dim_customer, etl_dim_employee, etl_dim_geography, etl_dim_product, etl_dim_salesterritory, load_dates, load_stg_sales] >> load_fct_sales