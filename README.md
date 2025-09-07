# Earthquake Visualization Pipeline with Streaming and Historical Data

A comprehensive earthquake and volcano monitoring pipeline that combines real-time streaming with historical data analysis. The system implements the medallion architecture on Delta Lake (stored on MinIO) and uses Kafka for streaming USGS earthquake events and NASA EONET volcano data into Spark, which powers an interactive dashboard. Historical datasets are curated in Bronze/Silver/Gold Delta tables to enable trend analysis alongside live updates.

## Architecture Overview

This pipeline implements a modern data architecture with:

- **Delta Lake**: ACID transactions, schema enforcement, and time travel on top of MinIO
- **Kafka**: Real-time streaming with topic-based messaging
- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **Spark**: Distributed data processing with standalone cluster
- **MinIO**: S3-compatible object storage
- **Apache Airflow**: Workflow orchestration and monitoring
- **PostgreSQL**: Airflow metadata database
- **Real-time Streaming**: Earthquake and volcano event processing with live analytics

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

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- At least 4GB RAM available for containers
- Ports 8080, 8081, 8050, 9000, 9001, 9092 available

### Option 1: Using Helper Scripts (Recommended)

```bash
# 1. Start the complete pipeline
./run_pipeline.sh start

# 2. Initialize Airflow (first time only)
./run_pipeline.sh init-airflow

# 3. Check pipeline status
./run_pipeline.sh status
```

### Option 2: Manual Docker Compose

```bash
# 1. Start all services
docker-compose up -d

# 2. Initialize Airflow database (first time only)
docker-compose up airflow-init

# 3. Check service status
docker-compose ps
```

### 2. Monitor the Pipeline

```bash
# Using helper script
./run_pipeline.sh status

# Or manually view logs
docker-compose logs -f streaming-service
docker-compose logs -f streaming-processor-bronze
docker-compose logs -f streaming-processor-volcano-bronze
docker-compose logs -f streaming-processor-silver
docker-compose logs -f streaming-processor-volcano-silver
docker-compose logs -f streaming-processor-gold
```

### 3. Access Services
- **MinIO Console**: http://localhost:9001 (minio/minio123)
- **Dashboard**: http://localhost:8050
- **Kafka**: localhost:9092
- **Spark UI**: http://localhost:8080
- **Airflow UI**: http://localhost:8081 (airflow/airflow)

## Helper Scripts

The project includes several helper scripts to simplify pipeline management:

### Pipeline Management (`run_pipeline.sh`)
Comprehensive script for managing the entire pipeline:

```bash
# Start the complete pipeline
./run_pipeline.sh start

# Stop the pipeline
./run_pipeline.sh stop

# Restart the pipeline
./run_pipeline.sh restart

# Check pipeline status and logs
./run_pipeline.sh status

# View logs for specific components
./run_pipeline.sh logs <component>

# Start specific components
./run_pipeline.sh component <component>

# Initialize Airflow (first time setup)
./run_pipeline.sh init-airflow

# Clean up (remove all containers, volumes, networks)
./run_pipeline.sh cleanup

# Show help
./run_pipeline.sh help
```

**Available Components**: `minio`, `kafka`, `zookeeper`, `streaming-service`, `visualization`, `airflow-webserver`, `airflow-scheduler`

### Database Setup (`run_database_setup.sh`)
Simplified script for initial database setup:

```bash
# Set up MinIO and initialize buckets
./run_database_setup.sh
```

### Pipeline Status Check (`check_pipeline_status.sh`)
Quick status check for all pipeline components:

```bash
# Check overall pipeline health
./check_pipeline_status.sh
```

## Services

### Core Infrastructure
- **MinIO**: S3-compatible object storage for Delta Lake tables
- **Kafka**: Message streaming platform for real-time data
- **Zookeeper**: Kafka coordination service
- **PostgreSQL**: Airflow metadata database

### Data Processing
- **Pipeline**: Batch processing with Delta Lake (World Bank data)
- **Streaming Service**: Real-time data ingestion from USGS and NASA APIs
- **Streaming Processors**: 
  - Bronze processors (earthquake and volcano data)
  - Silver processors (data cleaning and enrichment)
  - Gold processors (analytics and aggregations)

### Orchestration & Monitoring
- **Apache Airflow**: Workflow orchestration and monitoring
  - Airflow Webserver: Web UI for DAG management
  - Airflow Scheduler: Task scheduling and execution
  - Airflow Init: Database initialization

### Visualization
- **Dashboard**: Real-time earthquake and volcano monitoring with interactive maps

### Spark Cluster
- **Spark Master**: Cluster coordination and job scheduling
- **Spark Worker**: Distributed data processing

##  Streaming Pipeline

### Real-time Data Flow
1. **Streaming Service**: Fetches earthquake data from USGS API and volcano event data from NASA EONET
2. **Kafka Bronze Topics**: Receive raw earthquake and volcano data
3. **Bronze Processors**: Store raw data in Delta Lake (earthquakes and volcano events)
4. **Kafka Silver Topics**: Receive cleaned data (earthquakes and volcano events)
5. **Silver Processors**: Process and enrich data
6. **Kafka Gold Topics**: Receive aggregated analytics
7. **Gold Processors**: Create business insights and alerts

### Kafka Topics
- `earthquake-bronze`: Raw earthquake data
- `earthquake-silver`: Processed earthquake data
- `earthquake-gold`: Aggregated analytics
- `volcano-bronze`: Raw volcano event data
- `volcano-silver`: Processed volcano event data
- `volcano-gold`: Aggregated volcano insights

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

## Apache Airflow Orchestration

The pipeline includes comprehensive workflow orchestration using Apache Airflow with three main DAGs:

### Available DAGs

#### 1. Batch Pipeline DAG (`batch_pipeline_dag`)
- **Schedule**: Daily at 2:00 AM
- **Purpose**: Orchestrates batch processing of World Bank data through medallion layers
- **Tasks**:
  - Health checks for MinIO and Kafka
  - Bronze layer: Raw data ingestion
  - Silver layer: Data cleaning and validation
  - Gold layer: Analytics and aggregations
  - Delta table optimization
  - Data quality validation
  - Completion notifications

#### 2. Data Quality DAG (`data_quality_dag`)
- **Schedule**: Every 2 hours
- **Purpose**: Continuous monitoring and validation of data quality across all layers
- **Tasks**:
  - Validate data quality in Bronze, Silver, and Gold layers
  - Check earthquake data quality
  - Verify data completeness and freshness
  - Schema validation
  - Generate quality reports
  - Trigger data refresh if needed

#### 3. Streaming Monitoring DAG (`streaming_monitoring_dag`)
- **Schedule**: Every 5 minutes
- **Purpose**: Real-time monitoring of streaming pipeline health
- **Tasks**:
  - Check streaming service health
  - Verify Kafka topics and connectivity
  - Monitor streaming processors (Bronze, Silver, Gold)
  - Dashboard health checks
  - Data freshness validation
  - Delta Lake table verification
  - Generate monitoring reports
  - Alert on failures

### Airflow Access
- **Web UI**: http://localhost:8081
- **Default Credentials**: `airflow` / `airflow`
- **Database**: PostgreSQL (internal)

## Monitoring and Maintenance

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
- Real-time earthquake feeds from USGS API
- Magnitude and location data
- Tsunami warnings
- Significance scores
- Historical earthquake data
- GeoJSON format with comprehensive metadata

### NASA EONET Volcano Events
- **Source**: NASA Earth Observatory Natural Event Tracker (EONET)
- **Data Type**: Open volcano eruption events (active/open status)
- **Update Frequency**: Real-time updates
- **Data Includes**:
  - Event location and coordinates
  - Event category and type
  - Event lifecycle updates (opened/closed)
  - Event title and description
  - Source attribution and links
  - Event date and time information
- **Integration**: Streamed through Kafka topics and processed through medallion architecture

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

## Additional Resources

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

- NASA EONET (Earth Observatory Natural Event Tracker) — Volcano Events
  - Source: https://eonet.gsfc.nasa.gov/
  - API Terms/Use: https://api.nasa.gov/
  - License/Use: Generally U.S. Government work (not copyrighted) unless otherwise noted; attribution requested
  - Note: Cite NASA EONET for event listings; some events aggregate data from partner sources referenced on event detail pages.

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