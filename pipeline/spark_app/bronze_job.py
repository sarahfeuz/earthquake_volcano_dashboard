#!/usr/bin/env python3
"""
Bronze Layer - Data Ingestion
"""

import sys
import os
sys.path.append('/opt/spark_app')

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from data_fetcher import fetch_world_bank_data_main
from spark_utils import get_s3_client, write_parquet_table

def run_bronze_job(spark):
    """Run bronze layer data ingestion"""
    print("=" * 50)
    print("BRONZE LAYER - Data Ingestion (Parquet)")
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
    try:
        spark_df = spark.createDataFrame(world_bank_df)
        print(f"Successfully created Spark DataFrame with {spark_df.count()} rows")
    except Exception as e:
        print(f"Error creating Spark DataFrame: {e}")
        print(" DataFrame schema:")
        print(world_bank_df.dtypes)
        raise
    
    # Add metadata columns
    spark_df = spark_df.withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("data_source", lit("world_bank_api")) \
                      .withColumn("layer", lit("bronze"))
    
    # Print schema and preview
    print("Spark DataFrame schema:")
    spark_df.printSchema()
    
    print("World Bank data preview:")
    spark_df.show(10, truncate=False)
    
    # Create MinIO buckets if they don't exist
    s3_client = get_s3_client()
    try:
        s3_client.create_bucket(Bucket="bronze")
        print(" Created bronze bucket")
    except:
        print(" Bronze bucket already exists")
    
    # Write to bronze layer using Parquet (local storage for now)
    print("Writing to bronze layer using Parquet...")
    bronze_parquet_path = "/tmp/bronze/raw_data/world_bank"
    
    try:
        # Write as a single Parquet file to reduce memory pressure
        spark_df.coalesce(1).write.mode("overwrite").parquet(bronze_parquet_path)
        print(f"Successfully wrote {spark_df.count()} records to bronze layer")
        
        # Also write to MinIO using boto3 for now
        s3_client = get_s3_client()
        
        # Convert Spark DataFrame to pandas for easier upload
        pandas_df = spark_df.toPandas()
        
        # Write as CSV to MinIO
        csv_content = pandas_df.to_csv(index=False)
        s3_client.put_object(
            Bucket="bronze",
            Key="raw_data/world_bank_raw.csv",
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        print("Successfully uploaded CSV to MinIO bronze bucket")
        
    except Exception as e:
        print(f"Error writing to bronze layer: {e}")
        raise
    
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
