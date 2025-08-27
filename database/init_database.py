#!/usr/bin/env python3
"""
Database Initialization Script
Creates MinIO buckets and initializes the data pipeline
"""

import boto3
import time
from config.minio_config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKETS

def create_buckets():
    """Create all required MinIO buckets"""
    print("=" * 50)
    print("  INITIALIZING MINIO DATABASE")
    print("=" * 50)
    
    # Create S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    # Create buckets
    for bucket_name in BUCKETS.values():
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f" Created bucket: {bucket_name}")
        except Exception as e:
            if "AlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
                print(f"ℹ  Bucket already exists: {bucket_name}")
            else:
                print(f" Error creating bucket {bucket_name}: {e}")
    
    print("=" * 50)
    print(" Database initialization completed!")
    print("=" * 50)

if __name__ == "__main__":
    # Wait for MinIO to be ready
    print("Waiting for MinIO to be ready...")
    time.sleep(10)
    
    # Create buckets
    create_buckets() 