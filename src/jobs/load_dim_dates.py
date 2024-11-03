from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *

from datetime import datetime, timedelta
import logging

from config_services import get_default_SparkConf, SparkSchema
SCHEMAS = SparkSchema()


def main(start_date: datetime, end_date: datetime) -> None:
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Get current data in date table
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    df_cur = spark.sql(f"SELECT * FROM dimdate WHERE \
        date_date BETWEEN '{start_date_str}' AND '{end_date_str}';")
    expected_num_rows = (end_date - start_date + timedelta(days = 1)).days

    # Compare current data with expected - skip task if already have
    if df_cur.count() == expected_num_rows:
        print('#'*30)
        print(f"Data has already fully loaded. Ending...")
        return "skipped"

    # Populate date data 
    df_dates = populate_date_df(start_date_str, end_date_str)

    # Data to append
    df_append = df_dates.subtract(df_cur)
    
    # Write into DWH
    df_append.limit(10).show()        # for logging added data
    df_append.write \
        .mode("append") \
        .format("hive") \
        .saveAsTable("DimDate")
        

def populate_date_df(start_date: str, end_date: str) -> DataFrame:
    """Populate calendar date from start_date to end_date
    
    Schema of generated DataFrame:
    |--- date_key           (IntegerType())
    |--- date_date          (DateType())
    |--- year               (ShortType())
    |--- month              (ByteType())
    |--- day                (ByteType())
    |--- week_of_year       (ByteType())
    |--- day_of_week        (ByteType())
    """
    spark = SparkSession.getActiveSession()

    # Reference
    # (https://3cloudsolutions.com/resources/generate-a-calendar-dimension-in-spark/)
    spark.sql(f"""
        SELECT EXPLODE(
            SEQUENCE(
                TO_DATE('{start_date}'), 
                TO_DATE('{end_date}'), 
                INTERVAL 1 day
            )
        ) AS date_date;
    """) \
        .createOrReplaceTempView("dates")
    df_dates = spark.sql(f"""
        SELECT 
            (
                (YEAR(date_date) * 10000) 
                + MONTH(date_date) * 100 
                + DAY(date_date)
            ) AS date_key,
            date_date, 
            YEAR(date_date) AS year,
            MONTH(date_date) AS month,
            DAY(date_date) AS day,
            WEEKOFYEAR(date_date) AS week_of_year,
            DAYOFWEEK(date_date) AS day_of_week
        FROM dates;
    """)

    return df_dates


if __name__ == "__main__":
    import argparse
    import os

    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date",
        help = "date of data (in YYYY-MM-DD)"
    )
    parser.add_argument("end_date",
        help = "date of data (in YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Preliminary input validation
    try:
        args.start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        args.end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        logging.error("\"start_date\", \"end_date\" must be YYYY-MM-DD.")
        raise ValueError("Invalid input dates.")

    # Call main function
    main(start_date = args.start_date, end_date = args.end_date)