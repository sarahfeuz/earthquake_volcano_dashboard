from pyspark.sql import SparkSession
import boto3
import os
import glob
from config.minio_config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKETS

def get_spark_session(app_name="PipelineApp", use_s3=True):
    if use_s3:
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
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "2")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
    else:
        # Local-only Spark session without Delta Lake
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")  # Use local mode
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "2")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
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

def write_parquet_table(df, table_path, mode="overwrite", partition_by=None):
    """Write DataFrame to Parquet format"""
    try:
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        writer.parquet(table_path)
        print(f"Successfully wrote Parquet table to: {table_path}")
        
    except Exception as e:
        print(f"Error writing Parquet table: {e}")
        raise

def read_parquet_table(spark, table_path):
    """Read Parquet table from path"""
    try:
        df = spark.read.parquet(table_path)
        print(f"Successfully read Parquet table from: {table_path}")
        return df
    except Exception as e:
        print(f"Error reading Parquet table: {e}")
        raise
