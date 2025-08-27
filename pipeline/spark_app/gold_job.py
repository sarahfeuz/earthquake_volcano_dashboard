#!/usr/bin/env python3
"""
Gold Layer - Data Analytics
Creates aggregated analytics from processed World Bank data using Delta Lake
"""

import sys
import os
sys.path.append('/opt/spark_app')

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from spark_utils import get_s3_client, write_delta_table, read_delta_table

def run_gold_job(spark, silver_df=None):
    """Run the gold layer data analytics job"""
    print("=" * 50)
    print("GOLD LAYER - Data Analytics (Delta Lake)")
    print("=" * 50)
    
    print("Running Gold job...")
    
    if silver_df is None:
        # Read from silver layer Delta table if not provided
        print("Reading from silver layer Delta table...")
        silver_delta_path = "s3a://silver/processed_data/world_bank"
        silver_df = read_delta_table(spark, silver_delta_path)
    else:
        print("Using provided silver dataframe")
    
    # Create simplified gold layer aggregations
    print("Creating simplified gold layer aggregations...")
    
    # 1. Country-level aggregations by indicator category
    country_aggregations = silver_df.groupBy("country_code", "country_name", "indicator_category") \
        .agg(
            count("indicator_id").alias("indicator_count"),
            avg("value").alias("avg_value"),
            max("value").alias("max_value"),
            min("value").alias("min_value"),
            stddev("value").alias("stddev_value"),
            (count("value") / count("*") * 100).alias("completeness_rate")
        )
    
    # 2. Global averages for comparison
    global_avgs = silver_df.groupBy("indicator_category") \
        .agg(
            avg("value").alias("avg_global_avg"),
            max("value").alias("max_global_max"),
            min("value").alias("min_global_min")
        )
    
    # 3. Join country aggregations with global averages
    gold_df = country_aggregations.join(global_avgs, "indicator_category", "left")
    
    # 4. Add environmental score for disaster risk indicators
    disaster_risk_data = silver_df.filter(col("indicator_category") == "Disaster Risk")
    
    if disaster_risk_data.count() > 0:
        print("Environmental data available: True")
        # Calculate environmental vulnerability score
        env_scores = disaster_risk_data.groupBy("country_code") \
            .agg(
                avg("value").alias("environmental_score")
            )
        
        # Join with main gold data
        gold_df = gold_df.join(env_scores, "country_code", "left")
    else:
        print("Environmental data available: False")
        # Add null column for environmental score as double type
        gold_df = gold_df.withColumn("environmental_score", lit(None).cast("double"))
    
    # 5. Add trend analysis
    trend_data = silver_df.groupBy("country_code", "indicator_category") \
        .agg(
            avg(when(col("year") < 2018, col("value"))).alias("early_period_avg"),
            avg(when((col("year") >= 2018) & (col("year") < 2021), col("value"))).alias("mid_period_avg"),
            avg(when(col("year") >= 2021, col("value"))).alias("recent_period_avg")
        )
    
    # Join trend data
    gold_df = gold_df.join(trend_data, ["country_code", "indicator_category"], "left")
    
    # 6. Add risk assessment
    gold_df = gold_df.withColumn("risk_level",
                               when(col("environmental_score").isNotNull() & (col("environmental_score") > 3), "High Risk")
                               .when(col("environmental_score").isNotNull() & (col("environmental_score") > 2), "Medium Risk")
                               .when(col("environmental_score").isNotNull(), "Low Risk")
                               .otherwise("Unknown"))
    
    # Add analytics metadata
    gold_df = gold_df.withColumn("analytics_timestamp", current_timestamp()) \
                    .withColumn("layer", lit("gold")) \
                    .withColumn("analytics_version", lit("1.0"))
    
    # Show aggregated data
    print(f"Simplified gold dataset shape: {gold_df.count()} rows")
    print("Aggregated data preview:")
    gold_df.show(10, truncate=False)
    
    # Write to gold layer using Delta Lake
    print("Writing to gold layer using Delta Lake...")
    gold_delta_path = "s3a://gold/aggregated_data/world_bank"
    
    # Write as Delta table with partitioning by country_code and indicator_category
    write_delta_table(
        df=gold_df,
        table_path=gold_delta_path,
        mode="overwrite",
        partition_by=["country_code", "indicator_category"]
    )
    
    print(f" Gold job completed. Delta table written to: {gold_delta_path}")
    
    return gold_df

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("GoldLayer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        gold_df = run_gold_job(spark)
        if gold_df is not None:
            print(" Gold job completed successfully!")
        else:
            print(" Gold job failed!")
            sys.exit(1)
    except Exception as e:
        print(f" Error in gold job: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()