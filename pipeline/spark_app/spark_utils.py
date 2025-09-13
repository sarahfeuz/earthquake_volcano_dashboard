from pyspark.sql import SparkSession
import boto3
import os
import glob
from config.minio_config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKETS

def get_spark_session(app_name="PipelineApp", use_s3=True):
    if use_s3:
        # S3-enabled Spark session without Delta Lake (simplified)
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")  # Use local mode instead of cluster
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.connection.maximum", "1000")
            .config("spark.hadoop.fs.s3a.threads.max", "20")
            .getOrCreate()
        )
    else:
        # Local-only Spark session without Delta Lake
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")  # Use local mode
            .getOrCreate()
        )

    return spark

def get_s3_client():
    """Get S3 client for MinIO operations"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

def upload_to_s3(s3_client, local_path, bucket_name, prefix):
    """Upload files from local path to S3/MinIO"""
    try:
        # Find all parquet files in the local path
        parquet_files = glob.glob(os.path.join(local_path, "**/*.parquet"), recursive=True)
        
        if not parquet_files:
            print(f"No parquet files found in {local_path}")
            # Debug: list directory contents
            if os.path.exists(local_path):
                print(f"Directory contents of {local_path}:")
                for root, dirs, files in os.walk(local_path):
                    print(f"  {root}: {files}")
            return
        
        for parquet_file in parquet_files:
            # Create the S3 key (path in bucket)
            relative_path = os.path.relpath(parquet_file, local_path)
            s3_key = f"{prefix}/{relative_path}"
            
            # Upload file
            s3_client.upload_file(parquet_file, bucket_name, s3_key)
            print(f"Uploaded {os.path.basename(parquet_file)} to s3a://{bucket_name}/{s3_key}")
            
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def create_minio_buckets():
    """Create MinIO buckets if they don't exist"""
    s3_client = get_s3_client()
    
    for bucket_name in BUCKETS.values():
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Created bucket: {bucket_name}")
        except Exception as e:
            print(f"Bucket {bucket_name} already exists or error: {e}")

def get_spark(app_name="PipelineApp"):
    """Alias for get_spark_session for backward compatibility"""
    return get_spark_session(app_name, use_s3=True)

def write_delta_table(df, table_path, mode="overwrite", partition_by=None):
    """Write DataFrame to Delta table format"""
    try:
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        writer.save(table_path)
        print(f" Successfully wrote Delta table to: {table_path}")
        
    except Exception as e:
        print(f" Error writing Delta table: {e}")
        raise

def read_delta_table(spark, table_path):
    """Read Delta table from path"""
    try:
        df = spark.read.format("delta").load(table_path)
        print(f" Successfully read Delta table from: {table_path}")
        return df
    except Exception as e:
        print(f" Error reading Delta table: {e}")
        raise

def merge_delta_table(target_path, source_df, merge_key, when_matched="update", when_not_matched="insert"):
    """Merge data into Delta table"""
    try:
        from delta.tables import DeltaTable
        
        # Read existing Delta table
        delta_table = DeltaTable.forPath(spark, target_path)
        
        # Perform merge
        delta_table.alias("target").merge(
            source_df.alias("source"),
            merge_key
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        print(f" Successfully merged data into Delta table: {target_path}")
        
    except Exception as e:
        print(f" Error merging Delta table: {e}")
        raise

def optimize_delta_table(table_path):
    """Optimize Delta table for better performance"""
    try:
        spark.sql(f"OPTIMIZE '{table_path}'")
        print(f" Successfully optimized Delta table: {table_path}")
    except Exception as e:
        print(f" Error optimizing Delta table: {e}")
        raise

def vacuum_delta_table(table_path, retention_hours=168):  # Default 7 days
    """Vacuum Delta table to remove old files"""
    try:
        spark.sql(f"VACUUM '{table_path}' RETAIN {retention_hours} HOURS")
        print(f" Successfully vacuumed Delta table: {table_path}")
    except Exception as e:
        print(f" Error vacuuming Delta table: {e}")
        raise
