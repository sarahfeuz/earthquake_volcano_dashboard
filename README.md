# Earthquake Dashboard 

A comprehensive data pipeline that implements the medallion architecture using Delta Lake on MinIO with Kafka for real-time streaming data processing.

##  Architecture Overview

This pipeline implements a modern data architecture with:

- **Delta Lake**: ACID transactions, schema enforcement, and time travel on top of MinIO
- **Kafka**: Real-time streaming with topic-based messaging
- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **Spark**: Distributed data processing
- **MinIO**: S3-compatible object storage
- **Real-time Streaming**: Earthquake data processing with live analytics

##  Data Flow

```
USGS API → Kafka Bronze Topic → Bronze Delta Table
                ↓
Kafka Silver Topic → Silver Delta Table (Cleaned/Processed)
                ↓
Kafka Gold Topic → Gold Delta Table (Aggregated Analytics)
                ↓
Dashboard Visualization
```

##  Medallion Architecture Layers

### Bronze Layer (Raw Data)
- **Source**: USGS Earthquake API, World Bank API
- **Storage**: Delta Lake tables in MinIO
- **Format**: Raw, unprocessed data with metadata
- **Partitioning**: By year, country_code

### Silver Layer (Cleaned Data)
- **Source**: Bronze layer Delta tables
- **Storage**: Delta Lake tables in MinIO
- **Format**: Cleaned, validated, and enriched data
- **Partitioning**: By year, country_code, region, severity_level

### Gold Layer (Analytics)
- **Source**: Silver layer Delta tables
- **Storage**: Delta Lake tables in MinIO
- **Format**: Aggregated analytics and business metrics
- **Partitioning**: By country_code, indicator_category

##  Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+

### 2. Add .jar files
Please add the following .jar files in /pipeline/spark_jar (you will find them online)
- hadoop-aws-3.3.4.jar
- aws-java-sdk-bundle-1.12.262.jar

### 2. Start the Pipeline
```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 3. Monitor the Pipeline
```bash
# View logs
docker-compose logs -f streaming-service
docker-compose logs -f streaming-processor-bronze
docker-compose logs -f streaming-processor-silver
docker-compose logs -f streaming-processor-gold
```

### 4. Access Services
- **MinIO Console**: http://localhost:9001 (minio/minio123)
- **Dashboard**: http://localhost:8050
- **Kafka**: localhost:9092
- **Spark UI**: http://localhost:8080

##  Services

### Core Services
- **MinIO**: S3-compatible object storage
- **Kafka**: Message streaming platform
- **Zookeeper**: Kafka coordination service

### Data Processing
- **Pipeline**: Batch processing with Delta Lake
- **Streaming Service**: Real-time data ingestion
- **Streaming Processors**: Medallion architecture layers

### Visualization
- **Dashboard**: Real-time earthquake monitoring

##  Streaming Pipeline

### Real-time Data Flow
1. **Streaming Service**: Fetches earthquake data from USGS API
2. **Kafka Bronze Topic**: Receives raw earthquake data
3. **Bronze Processor**: Stores raw data in Delta Lake
4. **Kafka Silver Topic**: Receives cleaned earthquake data
5. **Silver Processor**: Processes and enriches data
6. **Kafka Gold Topic**: Receives aggregated analytics
7. **Gold Processor**: Creates business insights and alerts

### Kafka Topics
- `earthquake-bronze`: Raw earthquake data
- `earthquake-silver`: Processed earthquake data
- `earthquake-gold`: Aggregated analytics

##  Delta Lake Features

### ACID Transactions
- Atomic writes across partitions
- Consistent reads with snapshot isolation
- Durability with MinIO object storage

### Schema Enforcement
- Automatic schema validation
- Schema evolution support
- Data type enforcement

### Time Travel
- Point-in-time queries
- Data versioning
- Audit trail

### Performance Optimization
- Automatic file compaction
- Partition pruning
- Statistics collection

##  Monitoring and Maintenance

### Delta Lake Maintenance
```bash
# Optimize tables for better performance
docker-compose exec pipeline python3 -c "
from spark_app.spark_utils import optimize_delta_table
optimize_delta_table('s3a://bronze/raw_data/world_bank')
"

# Vacuum old files (keep last 7 days)
docker-compose exec pipeline python3 -c "
from spark_app.spark_utils import vacuum_delta_table
vacuum_delta_table('s3a://bronze/raw_data/world_bank', retention_hours=168)
"
```

### Kafka Monitoring
```bash
# List topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Monitor consumer groups
docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

##  Configuration

### Environment Variables
- `MINIO_ENDPOINT`: MinIO server endpoint
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses

### Delta Lake Configuration
- Automatic optimization enabled
- File compaction enabled
- Schema enforcement enabled
- Time travel enabled

##  Data Sources

### World Bank Data
- GDP indicators
- Population statistics
- Economic indicators
- Disaster risk metrics

### USGS Earthquake Data
- Real-time earthquake feeds
- Magnitude and location data
- Tsunami warnings
- Significance scores

##  Development

### Adding New Data Sources
1. Create new data fetcher in `pipeline/spark_app/data_fetcher.py`
2. Update bronze job to handle new source
3. Add processing logic in silver job
4. Create aggregations in gold job

### Extending Streaming Pipeline
1. Add new Kafka topics
2. Create new streaming processors
3. Update Delta Lake schemas
4. Extend dashboard visualizations


### Logs
```bash
# View all logs
docker-compose logs

# View specific service logs
docker-compose logs streaming-service
docker-compose logs pipeline
```

##  Additional Resources

- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [MinIO Documentation](https://docs.min.io/)
- [Apache Spark Documentation](https://spark.apache.org/docs/)

##  Contributing

Thank you for your interest, but this project is not open to public contributions. You are welcome to fork it for your own purposes.


##  License

No official license at this time. This repository is provided "as is" for demonstration and educational purposes only.

Please review and comply with all third‑party data and software licenses referenced below before using this project beyond personal evaluation.

##  Data Attribution & Terms

- World Bank Open Data
  - Source: https://data.worldbank.org/
  - License: Creative Commons Attribution 4.0 (CC BY 4.0)
    - Terms: https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets
  - Note: Attribution is required when using or sharing World Bank data.

- USGS Earthquake Hazards Program (GeoJSON Feeds)
  - Source: https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
  - License/Use: Public domain (U.S. Government work) with attribution requested
    - Policy: https://www.usgs.gov/information-policies-and-instructions/copyrights-and-credits
  - Note: Respect USGS disclaimers and usage guidance for the feeds.

##  Acknowledgments (Software Used)

- Apache Spark — https://spark.apache.org/
- Delta Lake — https://delta.io/
- Apache Kafka — https://kafka.apache.org/
- Apache ZooKeeper — https://zookeeper.apache.org/
- MinIO — https://min.io/
- Apache Airflow — https://airflow.apache.org/
- PostgreSQL — https://www.postgresql.org/
- Docker — https://www.docker.com/ and Docker Compose — https://docs.docker.com/compose/
- Python — https://www.python.org/
- Dash (Plotly) — https://dash.plotly.com/ and Plotly — https://plotly.com/python/
- Pandas — https://pandas.pydata.org/
- NumPy — https://numpy.org/
- boto3 — https://boto3.amazonaws.com/
- requests — https://requests.readthedocs.io/
- PySpark — included with Apache Spark
- kafka-python — https://kafka-python.readthedocs.io/
- confluent-kafka — https://docs.confluent.io/platform/current/clients/confluent-kafka-python/
