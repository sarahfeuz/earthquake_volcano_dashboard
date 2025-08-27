#!/usr/bin/env python3
"""
Bronze Layer - Data Ingestion
Fetches raw data from World Bank API and stores in bronze layer using Delta Lake
"""

import sys
import os
sys.path.append('/opt/spark_app')

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from data_fetcher import fetch_world_bank_data_main
from spark_utils import get_s3_client, write_delta_table

def run_bronze_job(spark):
    """Run the bronze layer data ingestion job"""
    print("=" * 50)
    print("BRONZE LAYER - Data Ingestion (Delta Lake)")
    print("=" * 50)
    
    print("Running Bronze job...")
    
    # Fetch World Bank data
    print("Fetching World Bank data...")
    world_bank_df = fetch_world_bank_data_main()
    
    if world_bank_df.empty:
        print(" No World Bank data fetched!")
        return None
    
    # Convert pandas DataFrame to Spark DataFrame
    print("Converting to Spark DataFrame...")
    spark_df = spark.createDataFrame(world_bank_df)
    
    # Add metadata columns
    spark_df = spark_df.withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("data_source", lit("world_bank_api")) \
                      .withColumn("layer", lit("bronze"))
    
    # Print a small preview of real data if available
    print("World Bank data preview:")
    spark_df.show(10, truncate=False)
    
    # Write to bronze layer using Delta Lake
    print("Writing to bronze layer using Delta Lake...")
    bronze_delta_path = "s3a://bronze/raw_data/world_bank"
    
    # Write as Delta table with partitioning by year
    write_delta_table(
        df=spark_df,
        table_path=bronze_delta_path,
        mode="overwrite",
        partition_by=["year"]
    )
    
    print(f" Bronze job completed. Delta table written to: {bronze_delta_path}")
    
    return spark_df

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("BronzeLayer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        bronze_df = run_bronze_job(spark)
        if bronze_df is not None:
            print(" Bronze job completed successfully!")
        else:
            print(" Bronze job failed!")
            sys.exit(1)
    except Exception as e:
        print(f" Error in bronze job: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
