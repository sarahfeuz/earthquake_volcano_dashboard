"""
Batch Pipeline DAG
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import requests
import json
import logging

# Default task arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_minio_health():
    """Return True if the MinIO service responds to a liveness check.

    This verifies object storage is available before attempting to read/write
    Delta tables during the batch run.
    """
    try:
        response = requests.get('http://minio:9000/minio/health/live', timeout=10)
        if response.status_code == 200:
            logging.info("MinIO is healthy")
            return True
        else:
            logging.error(f"MinIO health check failed: {response.status_code}")
            return False
    except Exception as e:
        logging.error(f"MinIO health check error: {e}")
        return False

def check_kafka_health():
    """Return True if a TCP connection to the Kafka broker can be opened.

    While this is a batch job, we still verify Kafka is reachable because some
    processes may publish events or rely on shared infrastructure.
    """
    try:
        # Simple check - try to connect to Kafka
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('kafka', 9092))
        sock.close()
        if result == 0:
            logging.info("Kafka is healthy")
            return True
        else:
            logging.error("Kafka health check failed")
            return False
    except Exception as e:
        logging.error(f"Kafka health check error: {e}")
        return False

def run_bronze_job():
    """Simulate the Bronze layer job for World Bank data.

    In a production pipeline, replace this with the actual Spark submit or
    module invocation that writes raw data into a Delta table.
    """
    logging.info("Starting Bronze layer job...")
    # This would typically call the actual bronze job
    # For now, we'll simulate the process
    logging.info("Bronze job completed - Raw World Bank data ingested to Delta Lake")
    return "bronze_completed"

def run_silver_job():
    """Simulate the Silver layer processing step.

    Replace with transformation logic that cleans and enriches Bronze data and
    writes it into the Silver Delta table.
    """
    logging.info("Starting Silver layer job...")
    # This would typically call the actual silver job
    logging.info("Silver job completed - Data cleaned and processed")
    return "silver_completed"

def run_gold_job():
    """Simulate the Gold layer analytics step.

    Replace with aggregation logic that produces analytics-ready outputs in the
    Gold Delta table.
    """
    logging.info("Starting Gold layer job...")
    # This would typically call the actual gold job
    logging.info("Gold job completed - Analytics and aggregations created")
    return "gold_completed"

def optimize_delta_tables():
    """Placeholder for Delta table optimization.

    In production, call optimize/vacuum routines to compact small files and
    improve query performance.
    """
    logging.info("Optimizing Delta Lake tables...")
    # This would call the optimize functions
    logging.info("Delta tables optimized")
    return "optimization_completed"

def validate_data_quality():
    """Placeholder for data quality checks across layers.

    Implement schema checks, record counts, null checks, or domain rules as
    appropriate for your datasets.
    """
    logging.info("Validating data quality...")
    # Add data quality checks here
    logging.info("Data quality validation completed")
    return "validation_completed"

def notify_completion():
    """Log a completion message or trigger an external notification.

    Integrate with email, chat, or incident tooling as needed.
    """
    logging.info("Batch pipeline completed successfully!")
    return "pipeline_completed"

# Create DAG
dag = DAG(
    'batch_pipeline_dag',
    default_args=default_args,
    description='Batch processing pipeline for World Bank data through medallion architecture',
    schedule_interval='0 2 * * *',  # Daily at 2 in the morning
    catchup=False,
    tags=['batch', 'world-bank', 'medallion'],
)

# Define tasks
start = DummyOperator(task_id='start', dag=dag)

check_minio = PythonOperator(
    task_id='check_minio_health',
    python_callable=check_minio_health,
    dag=dag
)

check_kafka = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag
)

bronze_job = PythonOperator(
    task_id='bronze_job',
    python_callable=run_bronze_job,
    dag=dag
)

silver_job = PythonOperator(
    task_id='silver_job',
    python_callable=run_silver_job,
    dag=dag
)

gold_job = PythonOperator(
    task_id='gold_job',
    python_callable=run_gold_job,
    dag=dag
)

optimize_tables = PythonOperator(
    task_id='optimize_delta_tables',
    python_callable=optimize_delta_tables,
    dag=dag
)

validate_quality = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

notify = PythonOperator(
    task_id='notify_completion',
    python_callable=notify_completion,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start >> [check_minio, check_kafka]
[check_minio, check_kafka] >> bronze_job
bronze_job >> silver_job
silver_job >> gold_job
gold_job >> optimize_tables
optimize_tables >> validate_quality
validate_quality >> notify
notify >> end 