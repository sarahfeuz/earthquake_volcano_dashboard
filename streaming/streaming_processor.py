#!/usr/bin/env python3
"""
Streaming Data Processor
"""

import json
import time
import os
import sys
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

def get_spark_session():
    """Create and configure a SparkSession for Delta on MinIO.

    The session includes Delta Lake extensions and S3A settings so Spark can
    read and write Delta tables stored in MinIO via the S3-compatible API.
    """
    spark = SparkSession.builder \
        .appName("StreamingProcessor") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.databricks.delta.constraints.enabled", "false") \
        .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true") \
        .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true") \
        .config("spark.databricks.delta.autoOptimize.autoCompact", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()
    
    return spark

def get_kafka_consumer(topic, group_id):
    """Create a Kafka consumer for a given topic and consumer group.

    Messages are deserialized from JSON into Python objects for processing.
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return None

def get_kafka_producer():
    """Create a Kafka producer that emits JSON-encoded messages."""
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

def process_earthquake_bronze_to_silver(spark, earthquake_data):
    """Transform raw earthquake GeoJSON into Silver records and write to Delta.

    - Extracts core fields and coordinates
    - Adds severity and region classification
    - Writes partitioned Delta data by region and severity
    Returns the processed Python list for further publishing to Kafka.
    """
    try:
        # Convert to Spark DataFrame
        features = earthquake_data.get('features', [])
        if not features:
            return []
        
        # Extract properties and geometry
        processed_data = []
        for feature in features:
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            coordinates = geometry.get('coordinates', [0, 0, 0])
            
            # Process earthquake data
            earthquake = {
                'id': properties.get('id'),
                'timestamp': datetime.fromtimestamp(properties.get('time', 0) / 1000).isoformat(),
                'magnitude': properties.get('mag'),
                'place': properties.get('place'),
                'latitude': coordinates[1] if len(coordinates) > 1 else None,
                'longitude': coordinates[0] if len(coordinates) > 0 else None,
                'depth': coordinates[2] if len(coordinates) > 2 else None,
                'status': properties.get('status'),
                'alert': properties.get('alert'),
                'tsunami': properties.get('tsunami'),
                'significance': properties.get('sig'),
                'title': properties.get('title'),
                'url': properties.get('url'),
                'processing_timestamp': datetime.now().isoformat(),
                'layer': 'silver',
                'processing_version': '1.0'
            }
            
            # Add severity level
            mag = earthquake['magnitude']
            if mag is None:
                earthquake['severity_level'] = 'unknown'
            elif mag < 2.0:
                earthquake['severity_level'] = 'micro'
            elif mag < 4.0:
                earthquake['severity_level'] = 'minor'
            elif mag < 5.0:
                earthquake['severity_level'] = 'light'
            elif mag < 6.0:
                earthquake['severity_level'] = 'moderate'
            elif mag < 7.0:
                earthquake['severity_level'] = 'strong'
            elif mag < 8.0:
                earthquake['severity_level'] = 'major'
            else:
                earthquake['severity_level'] = 'great'
            
            # Add region based on coordinates
            lat, lon = earthquake['latitude'], earthquake['longitude']
            if lat and lon:
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    if lat > 60:
                        earthquake['region'] = 'Arctic'
                    elif lat < -60:
                        earthquake['region'] = 'Antarctic'
                    elif 23.5 <= lat <= 60:
                        earthquake['region'] = 'Northern Temperate'
                    elif -23.5 <= lat <= 23.5:
                        earthquake['region'] = 'Tropical'
                    else:
                        earthquake['region'] = 'Southern Temperate'
                else:
                    earthquake['region'] = 'Unknown'
            else:
                earthquake['region'] = 'Unknown'
            
            processed_data.append(earthquake)
        
        # Write to Delta Lake
        if processed_data:
            df = spark.createDataFrame(processed_data)
            df.write.format("delta") \
                .mode("append") \
                .partitionBy("region", "severity_level") \
                .save("s3a://silver/processed_data/earthquakes")
            
            logger.info(f"Processed {len(processed_data)} earthquakes to Silver layer")
        
        return processed_data
        
    except Exception as e:
        logger.error(f"Error processing earthquake data: {e}")
        return []

def process_volcano_bronze_to_silver(spark, volcano_data):
    """Transform raw volcano events into Silver records and write to Delta.

    - Extracts latest geometry and relevant fields
    - Derives simple severity and region
    - Writes partitioned Delta data by region and severity
    Returns the processed Python list for further publishing to Kafka.
    """
    try:
        events = volcano_data.get('events', [])
        if not events:
            return []
        
        processed_data = []
        for event in events:
            # Extract volcano data
            volcano = {
                'id': event.get('id'),
                'title': event.get('title'),
                'description': event.get('description'),
                'category': event.get('category'),
                'status': event.get('status'),
                'processing_timestamp': datetime.now().isoformat(),
                'layer': 'silver',
                'processing_version': '1.0'
            }
            
            # Extract geometry
            geometries = event.get('geometry', [])
            if geometries:
                # Get the most recent geometry
                latest_geom = geometries[-1]
                volcano['latitude'] = latest_geom.get('coordinates', [0, 0])[1]
                volcano['longitude'] = latest_geom.get('coordinates', [0, 0])[0]
                volcano['date'] = latest_geom.get('date')
            else:
                volcano['latitude'] = None
                volcano['longitude'] = None
                volcano['date'] = None
            
            # Add severity level based on title
            title = event.get('title', '').lower()
            if 'major' in title or 'large' in title:
                volcano['severity_level'] = 'major'
            elif 'minor' in title or 'small' in title:
                volcano['severity_level'] = 'minor'
            elif 'moderate' in title:
                volcano['severity_level'] = 'moderate'
            else:
                volcano['severity_level'] = 'unknown'
            
            # Add region based on coordinates
            lat, lon = volcano['latitude'], volcano['longitude']
            if lat and lon:
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    if lat > 60:
                        volcano['region'] = 'Arctic'
                    elif lat < -60:
                        volcano['region'] = 'Antarctic'
                    elif 23.5 <= lat <= 60:
                        volcano['region'] = 'Northern Temperate'
                    elif -23.5 <= lat <= 23.5:
                        volcano['region'] = 'Tropical'
                    else:
                        volcano['region'] = 'Southern Temperate'
                else:
                    volcano['region'] = 'Unknown'
            else:
                volcano['region'] = 'Unknown'
            
            processed_data.append(volcano)
        
        # Write to Delta Lake
        if processed_data:
            df = spark.createDataFrame(processed_data)
            df.write.format("delta") \
                .mode("append") \
                .partitionBy("region", "severity_level") \
                .save("s3a://silver/processed_data/volcanoes")
            
            logger.info(f"Processed {len(processed_data)} volcanoes to Silver layer")
        
        return processed_data
        
    except Exception as e:
        logger.error(f"Error processing volcano data: {e}")
        return []

def process_silver_to_gold(spark, silver_data, data_type):
    """Aggregate Silver records into Gold analytics and write to Delta.

    For earthquakes, produces counts and magnitude stats by region and severity.
    For volcanoes, produces counts and simple title lists by region and severity.
    Returns a list of aggregated records for fanout to Kafka.
    """
    try:
        if not silver_data:
            return []
        
        df = spark.createDataFrame(silver_data)
        
        if data_type == 'earthquake':
            # Aggregate earthquake data
            aggregated = df.groupBy("region", "severity_level") \
                .agg(
                    count("*").alias("earthquake_count"),
                    avg("magnitude").alias("avg_magnitude"),
                    max("magnitude").alias("max_magnitude"),
                    min("magnitude").alias("min_magnitude"),
                    avg("depth").alias("avg_depth"),
                    sum(when(col("tsunami") == 1, 1).otherwise(0)).alias("tsunami_count")
                )
            
            # Add analytics metadata
            aggregated = aggregated.withColumn("analytics_timestamp", lit(datetime.now().isoformat())) \
                .withColumn("layer", lit("gold")) \
                .withColumn("analytics_version", lit("1.0")) \
                .withColumn("data_type", lit("earthquake"))
            
            # Write to Delta Lake
            aggregated.write.format("delta") \
                .mode("append") \
                .partitionBy("region", "severity_level") \
                .save("s3a://gold/aggregated_data/earthquakes")
            
            logger.info(f"Aggregated {len(silver_data)} earthquakes to Gold layer")
            
        elif data_type == 'volcano':
            # Aggregate volcano data
            aggregated = df.groupBy("region", "severity_level") \
                .agg(
                    count("*").alias("volcano_count"),
                    collect_list("title").alias("volcano_titles")
                )
            
            # Add analytics metadata
            aggregated = aggregated.withColumn("analytics_timestamp", lit(datetime.now().isoformat())) \
                .withColumn("layer", lit("gold")) \
                .withColumn("analytics_version", lit("1.0")) \
                .withColumn("data_type", lit("volcano"))
            
            # Write to Delta Lake
            aggregated.write.format("delta") \
                .mode("append") \
                .partitionBy("region", "severity_level") \
                .save("s3a://gold/aggregated_data/volcanoes")
            
            logger.info(f"Aggregated {len(silver_data)} volcanoes to Gold layer")
        
        return aggregated.collect()
        
    except Exception as e:
        logger.error(f"Error processing to Gold layer: {e}")
        return []

def bronze_consumer():
    """Consume raw earthquake events, produce Silver-ready data to Kafka."""
    logger.info("Starting Bronze consumer...")
    
    spark = get_spark_session()
    consumer = get_kafka_consumer('earthquake-bronze', 'bronze-consumer-group')
    producer = get_kafka_producer()
    
    if not consumer or not producer:
        logger.error("Failed to initialize Kafka consumer/producer")
        return
    
    try:
        for message in consumer:
            data = message.value
            logger.info(f"Processing earthquake data: {len(data.get('features', []))} features")
            
            # Process to Silver layer
            silver_data = process_earthquake_bronze_to_silver(spark, data)
            
            # Send to Silver topic
            if silver_data:
                producer.send('earthquake-silver', value=silver_data, key='earthquake-silver')
                producer.flush()
                
    except KeyboardInterrupt:
        logger.info("Bronze consumer stopped")
    finally:
        consumer.close()
        producer.close()
        spark.stop()

def volcano_bronze_consumer():
    """Consume raw volcano events, produce Silver-ready data to Kafka."""
    logger.info("Starting Volcano Bronze consumer...")
    
    spark = get_spark_session()
    consumer = get_kafka_consumer('volcano-bronze', 'volcano-bronze-consumer-group')
    producer = get_kafka_producer()
    
    if not consumer or not producer:
        logger.error("Failed to initialize Kafka consumer/producer")
        return
    
    try:
        for message in consumer:
            data = message.value
            logger.info(f"Processing volcano data: {len(data.get('events', []))} events")
            
            # Process to Silver layer
            silver_data = process_volcano_bronze_to_silver(spark, data)
            
            # Send to Silver topic
            if silver_data:
                producer.send('volcano-silver', value=silver_data, key='volcano-silver')
                producer.flush()
                
    except KeyboardInterrupt:
        logger.info("Volcano Bronze consumer stopped")
    finally:
        consumer.close()
        producer.close()
        spark.stop()

def silver_consumer():
    """Consume Silver earthquake data, aggregate to Gold, publish to Kafka."""
    logger.info("Starting Silver consumer...")
    
    spark = get_spark_session()
    consumer = get_kafka_consumer('earthquake-silver', 'silver-consumer-group')
    producer = get_kafka_producer()
    
    if not consumer or not producer:
        logger.error("Failed to initialize Kafka consumer/producer")
        return
    
    try:
        for message in consumer:
            data = message.value
            logger.info(f"Processing {len(data)} earthquake records to Gold layer")
            
            # Process to Gold layer
            gold_data = process_silver_to_gold(spark, data, 'earthquake')
            
            # Send to Gold topic
            if gold_data:
                producer.send('earthquake-gold', value=gold_data, key='earthquake-gold')
                producer.flush()
                
    except KeyboardInterrupt:
        logger.info("Silver consumer stopped")
    finally:
        consumer.close()
        producer.close()
        spark.stop()

def volcano_silver_consumer():
    """Consume Silver volcano data, aggregate to Gold, publish to Kafka."""
    logger.info("Starting Volcano Silver consumer...")
    
    spark = get_spark_session()
    consumer = get_kafka_consumer('volcano-silver', 'volcano-silver-consumer-group')
    producer = get_kafka_producer()
    
    if not consumer or not producer:
        logger.error("Failed to initialize Kafka consumer/producer")
        return
    
    try:
        for message in consumer:
            data = message.value
            logger.info(f"Processing {len(data)} volcano records to Gold layer")
            
            # Process to Gold layer
            gold_data = process_silver_to_gold(spark, data, 'volcano')
            
            # Send to Gold topic
            if gold_data:
                producer.send('volcano-gold', value=gold_data, key='volcano-gold')
                producer.flush()
                
    except KeyboardInterrupt:
        logger.info("Volcano Silver consumer stopped")
    finally:
        consumer.close()
        producer.close()
        spark.stop()

def gold_consumer():
    """Consume Gold earthquake analytics for downstream sinks or inspection."""
    logger.info("Starting Gold consumer...")
    
    consumer = get_kafka_consumer('earthquake-gold', 'gold-consumer-group')
    
    if not consumer:
        logger.error("Failed to initialize Kafka consumer")
        return
    
    try:
        for message in consumer:
            data = message.value
            logger.info(f"Received Gold layer data: {len(data)} aggregated records")
            
            # Here you could send to external systems, generate reports, etc.
            for record in data:
                logger.info(f"Gold record: {record}")
                
    except KeyboardInterrupt:
        logger.info("Gold consumer stopped")
    finally:
        consumer.close()

def run_streaming_processor():
    """Entry point: dispatch to the requested layer-specific consumer.

    Usage:
      python streaming_processor.py <layer>
    Layers:
      - bronze, volcano-bronze
      - silver, volcano-silver
      - gold
    """
    if len(sys.argv) != 2:
        print("Usage: python streaming_processor.py <layer>")
        print("Layers: bronze, volcano-bronze, silver, volcano-silver, gold")
        sys.exit(1)
    
    layer = sys.argv[1]
    
    if layer == 'bronze':
        bronze_consumer()
    elif layer == 'volcano-bronze':
        volcano_bronze_consumer()
    elif layer == 'silver':
        silver_consumer()
    elif layer == 'volcano-silver':
        volcano_silver_consumer()
    elif layer == 'gold':
        gold_consumer()
    else:
        print(f"Unknown layer: {layer}")
        sys.exit(1)

if __name__ == "__main__":
    run_streaming_processor() 