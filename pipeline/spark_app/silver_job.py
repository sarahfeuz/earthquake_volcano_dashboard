#!/usr/bin/env python3
"""
Silver Layer - Data Processing
Cleans and processes World Bank data from bronze layer using Delta Lake
"""

import sys
import os
sys.path.append('/opt/spark_app')

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from spark_utils import get_s3_client, write_delta_table, read_delta_table

def run_silver_job(spark, bronze_df=None):
    """Run the silver layer data processing job"""
    print("=" * 50)
    print("SILVER LAYER - Data Processing (Delta Lake)")
    print("=" * 50)
    
    print("Running Silver job...")
    
    if bronze_df is None:
        # Read from bronze layer Delta table if not provided
        print("Reading from bronze layer Delta table...")
        bronze_delta_path = "s3a://bronze/raw_data/world_bank"
        bronze_df = read_delta_table(spark, bronze_delta_path)
    else:
        print("Using provided bronze dataframe")
    
    # Clean and process the data
    print("Cleaning and processing World Bank data...")
    
    # Remove null values
    silver_df = bronze_df.dropna(subset=['value', 'year', 'country_code'])
    
    # Convert year to integer and filter valid years
    silver_df = silver_df.withColumn('year', col('year').cast('int'))
    silver_df = silver_df.filter((col('year') >= 2015) & (col('year') <= 2025))
    
    # Add data quality flags
    silver_df = silver_df.withColumn('data_quality_flag', 
                                   when(col('value').isNull(), 'Missing Value')
                                   .when(col('value') < 0, 'Negative Value')
                                   .when(col('indicator_id').isNull(), 'Missing Indicator')
                                   .otherwise('Valid'))
    
    # Add completeness flag
    silver_df = silver_df.withColumn('is_complete', 
                                   when(col('country_code').isNotNull() & 
                                        col('indicator_id').isNotNull() & 
                                        col('year').isNotNull() & 
                                        col('value').isNotNull(), True)
                                   .otherwise(False))
    
    # Add derived fields for analysis
    silver_df = silver_df.withColumn('value_category',
                                   when(col('value') < 0, 'Negative')
                                   .when(col('value') == 0, 'Zero')
                                   .when(col('value') < 100, 'Low')
                                   .when(col('value') < 1000, 'Medium')
                                   .otherwise('High'))
    
    # Add year category for trend analysis
    silver_df = silver_df.withColumn('year_category',
                                   when(col('year') < 2018, 'Early Period')
                                   .when(col('year') < 2021, 'Mid Period')
                                   .otherwise('Recent Period'))
    
    # Add processing metadata
    silver_df = silver_df.withColumn("processing_timestamp", current_timestamp()) \
                        .withColumn("layer", lit("silver")) \
                        .withColumn("processing_version", lit("1.0"))
    
    # Show processing results
    total_records = silver_df.count()
    print(f"Data cleaning completed. Records: {total_records}")
    
    # Show a small preview of processed data
    print("Processed data preview:")
    silver_df.show(10, truncate=False)
    
    # Write to silver layer using Delta Lake
    print("Writing to silver layer using Delta Lake...")
    silver_delta_path = "s3a://silver/processed_data/world_bank"
    
    # Write as Delta table with partitioning by year and country_code
    write_delta_table(
        df=silver_df,
        table_path=silver_delta_path,
        mode="overwrite",
        partition_by=["year", "country_code"]
    )
    
    print(f" Silver job completed. Delta table written to: {silver_delta_path}")
    
    return silver_df

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SilverLayer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        silver_df = run_silver_job(spark)
        if silver_df is not None:
            print(" Silver job completed successfully!")
        else:
            print(" Silver job failed!")
            sys.exit(1)
    except Exception as e:
        print(f" Error in silver job: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
