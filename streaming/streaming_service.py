#!/usr/bin/env python3
"""
Streaming Data Service
Fetches real-time earthquake data and volcano eruption data from APIs
"""

import requests
import json
import time
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
BRONZE_TOPIC = 'earthquake-bronze'
VOLCANO_BRONZE_TOPIC = 'volcano-bronze'
SILVER_TOPIC = 'earthquake-silver'
GOLD_TOPIC = 'earthquake-gold'

def get_kafka_producer():
    """Create and return a Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None

def send_to_kafka(producer, topic, data, key=None):
    """Send data to Kafka topic"""
    try:
        if producer:
            producer.send(topic, value=data, key=key)
            producer.flush()
            logger.info(f"Sent data to {topic}")
        else:
            logger.warning("Kafka producer not available")
    except Exception as e:
        logger.error(f"Failed to send data to Kafka: {e}")

def upload_to_minio(data, filename, data_type='earthquake'):
    """Upload data to MinIO"""
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # Determine the path based on data type
        if data_type == 'volcano':
            s3_key = f'volcano_data/{filename}'
        else:
            s3_key = f'earthquake_data/{filename}'
        
        # Upload data
        s3_client.put_object(
            Bucket='streaming',
            Key=s3_key,
            Body=json.dumps(data, indent=2)
        )
        logger.info(f"Uploaded {filename} to MinIO at {s3_key}")
        
    except Exception as e:
        logger.error(f"Failed to upload to MinIO: {e}")

def fetch_earthquake_data():
    """Fetch earthquake data from USGS API"""
    try:
        # Get earthquakes from the last 24 hours
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        
        url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        params = {
            'format': 'geojson',
            'starttime': start_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'endtime': end_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'minmagnitude': 2.5
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Fetched {len(data.get('features', []))} earthquakes")
        return data
        
    except Exception as e:
        logger.error(f"Failed to fetch earthquake data: {e}")
        return None

def fetch_volcano_data():
    """Fetch volcano eruption data from NASA EONET API"""
    try:
        url = "https://eonet.gsfc.nasa.gov/api/v3/events"
        params = {
            'category': 'volcanoes',
            'status': 'open'  # Only get open events
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        events = data.get('events', [])
        
        # Filter for actual eruptions (not just volcanic activity)
        eruptions = []
        for event in events:
            # Check if it's an eruption event
            title = event.get('title', '').lower()
            if 'eruption' in title or 'volcanic' in title or 'volcano' in title:
                eruptions.append(event)
        
        logger.info(f"Fetched {len(eruptions)} volcano eruptions")
        return {'events': eruptions}
        
    except Exception as e:
        logger.error(f"Failed to fetch volcano data: {e}")
        return None

def run_streaming_service():
    """Main streaming service function"""
    logger.info("Starting streaming service...")
    
    # Initialize Kafka producer
    producer = get_kafka_producer()
    
    while True:
        try:
            # Fetch earthquake data
            earthquake_data = fetch_earthquake_data()
            if earthquake_data:
                # Send to Kafka
                send_to_kafka(producer, BRONZE_TOPIC, earthquake_data, 'earthquake')
                
                # Upload to MinIO for backup
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'earthquakes_{timestamp}.json'
                upload_to_minio(earthquake_data, filename, 'earthquake')
            
            # Fetch volcano data
            volcano_data = fetch_volcano_data()
            if volcano_data:
                # Send to Kafka
                send_to_kafka(producer, VOLCANO_BRONZE_TOPIC, volcano_data, 'volcano')
                
                # Upload to MinIO for backup
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'volcanoes_{timestamp}.json'
                upload_to_minio(volcano_data, filename, 'volcano')
            
            # Wait 5 minutes before next fetch
            logger.info("Waiting 5 minutes before next data fetch...")
            time.sleep(300)
            
        except KeyboardInterrupt:
            logger.info("Streaming service stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in streaming service: {e}")
            time.sleep(60)  # Wait 1 minute before retrying

if __name__ == "__main__":
    print(" Starting Earthquake & Volcano Streaming Service...")
    print("=" * 60)
    print(f" USGS API: Earthquake data")
    print(f" NASA EONET API: Volcano data")
    print(f" MinIO Bucket: streaming")
    print(f" Kafka Topics: earthquake-bronze, volcano-bronze")
    print("=" * 60)
    run_streaming_service() 