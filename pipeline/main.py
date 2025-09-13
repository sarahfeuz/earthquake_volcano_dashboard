#!/usr/bin/env python3
"""
Main Data Pipeline with Delta Lake
Orchestrates the Bronze, Silver, and Gold data processing jobs using Delta Lake
"""

from spark_app.bronze_job import run_bronze_job
from spark_app.silver_job import run_silver_job
from spark_app.gold_job import run_gold_job
from spark_app.spark_utils import get_spark_session, optimize_delta_table, vacuum_delta_table

def main():
    """
    Main pipeline execution with Delta Lake
    """
    print("Starting Data Pipeline with Delta Lake...")
    
    # Initialize Spark session
    print("Initializing Spark session with Delta Lake support...")
    spark = get_spark_session(use_s3=True)
    
    try:
        # Bronze Layer: Fetch World Bank data
        print("\n" + "="*50)
        print("BRONZE LAYER - Data Ingestion (Delta Lake)")
        print("="*50)
        bronze_df = run_bronze_job(spark)
        
        # Silver Layer: Data cleaning and processing
        print("\n" + "="*50)
        print("SILVER LAYER - Data Processing (Delta Lake)")
        print("="*50)
        silver_df = run_silver_job(spark, bronze_df)
        
        # Gold Layer: Data aggregation and insights
        print("\n" + "="*50)
        print("GOLD LAYER - Data Analytics (Delta Lake)")
        print("="*50)
        gold_df = run_gold_job(spark, silver_df)
        
        # Data maintenance operations
        print("\n" + "="*50)
        print("DATA MAINTENANCE")
        print("="*50)
        
        print("Data processing completed successfully!")
        
        print("\n" + "="*50)
        print(" PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*50)
        print(f" Bronze records: {bronze_df.count()}")
        print(f" Silver records: {silver_df.count()}")
        print(f" Gold records: {gold_df.count()}")
        print("\n Access your data:")
        print("- MinIO Console: http://localhost:9001")
        print("- Visualization Dashboard: http://localhost:8050")
        print("- Kafka Topics: earthquake-bronze, earthquake-silver, earthquake-gold")
        print("\n Delta Lake Tables:")
        print("- Bronze: s3a://bronze/raw_data/world_bank")
        print("- Silver: s3a://silver/processed_data/world_bank")
        print("- Gold: s3a://gold/aggregated_data/world_bank")
        
    except Exception as e:
        print(f" Pipeline failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

