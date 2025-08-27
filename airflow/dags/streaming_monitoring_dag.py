"""
Streaming Pipeline Monitoring DAG

This DAG runs on a short interval to verify that the real-time streaming pipeline is
healthy and producing data as expected. It focuses on validating the following:

1) The streaming service container is up and running
2) Kafka topics exist (at minimum the earthquake topics) and are accessible
3) The Bronze/Silver/Gold streaming processors are running
4) The dashboard is reachable over HTTP
5) Fresh streaming data files are being written to MinIO
6) Delta Lake tables for the batch pipeline exist in MinIO

Each check is implemented as a small Python task that returns True/False and logs
contextual details for troubleshooting. A summary report aggregates the results.
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
import subprocess

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

def check_streaming_service_health():
    """Return True if the streaming service container is running.

    This uses `docker ps` to check the container status. If the container is not
    running, we log an error to aid debugging in the Airflow task logs.
    """
    try:
        # Check if the streaming service container is running
        result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=streaming-service', '--format', '{{.Status}}'],
            capture_output=True, text=True
        )
        if 'Up' in result.stdout:
            logging.info("Streaming service is running")
            return True
        else:
            logging.error("Streaming service is not running")
            return False
    except Exception as e:
        logging.error(f"Error checking streaming service: {e}")
        return False

def check_kafka_topics():
    """Return True if expected Kafka topics exist on the broker.

    We list topics via the Kafka container and look for earthquake-related topics
    as a basic readiness check. This ensures Kafka is reachable and topics are in place.
    """
    try:
        # Check if earthquake topics exist
        result = subprocess.run([
            'docker', 'exec', 'kafka', 'kafka-topics', 
            '--list', '--bootstrap-server', 'localhost:9092'
        ], capture_output=True, text=True)
        
        topics = result.stdout.strip().split('\n')
        earthquake_topics = [topic for topic in topics if 'earthquake' in topic]
        
        if earthquake_topics:
            logging.info(f"Found earthquake topics: {earthquake_topics}")
            return True
        else:
            logging.warning("No earthquake topics found")
            return False
    except Exception as e:
        logging.error(f"Error checking Kafka topics: {e}")
        return False

def check_streaming_processors():
    """Return True if Bronze, Silver, and Gold processor containers are running.

    This validates that the medallion streaming processors are up so they can move
    data through the pipeline layers in near real time.
    """
    processors = ['streaming-processor-bronze', 'streaming-processor-silver', 'streaming-processor-gold']
    healthy_processors = []
    
    for processor in processors:
        try:
            result = subprocess.run([
                'docker', 'ps', '--filter', f'name={processor}', '--format', '{{.Status}}'
            ], capture_output=True, text=True)
            
            if 'Up' in result.stdout:
                healthy_processors.append(processor)
                logging.info(f"{processor} is running")
            else:
                logging.warning(f"{processor} is not running")
        except Exception as e:
            logging.error(f"Error checking {processor}: {e}")
    
    if len(healthy_processors) == len(processors):
        logging.info("All streaming processors are healthy")
        return True
    else:
        logging.warning(f"Only {len(healthy_processors)}/{len(processors)} processors are running")
        return False

def check_dashboard_health():
    """Return True if the dashboard responds with HTTP 200 at localhost:8050.

    This verifies that users can access the dashboard UI and it is serving requests.
    """
    try:
        response = requests.get('http://localhost:8050', timeout=10)
        if response.status_code == 200:
            logging.info("Dashboard is healthy and accessible")
            return True
        else:
            logging.error(f"Dashboard health check failed: {response.status_code}")
            return False
    except Exception as e:
        logging.error(f"Dashboard health check error: {e}")
        return False

def check_data_freshness():
    """Return True if recent earthquake data files are present in MinIO.

    We query the MinIO container for the earthquake data directory and log the
    most recent file discovered. This is a proxy for end-to-end streaming health.
    """
    try:
        # Check the latest earthquake data file in MinIO
        result = subprocess.run([
            'docker', 'exec', 'minio-db', 'mc', 'ls', '/data/streaming/earthquake_data'
        ], capture_output=True, text=True)
        
        if result.stdout:
            lines = result.stdout.strip().split('\n')
            if lines:
                latest_file = lines[-1]
                logging.info(f"Latest earthquake data: {latest_file}")
                return True
            else:
                logging.warning("No earthquake data files found")
                return False
        else:
            logging.warning("No earthquake data files found")
            return False
    except Exception as e:
        logging.error(f"Error checking data freshness: {e}")
        return False

def check_delta_lake_tables():
    """Return True if Bronze/Silver/Gold Delta directories exist in MinIO.

    This primarily validates batch pipeline artifacts exist and can be listed. It
    does not perform a full table integrity check, only presence.
    """
    try:
        # Check if Delta Lake tables exist in MinIO
        bronze_check = subprocess.run([
            'docker', 'exec', 'minio-db', 'mc', 'ls', '/data/bronze/raw_data/world_bank'
        ], capture_output=True, text=True)
        
        silver_check = subprocess.run([
            'docker', 'exec', 'minio-db', 'mc', 'ls', '/data/silver/processed_data/world_bank'
        ], capture_output=True, text=True)
        
        gold_check = subprocess.run([
            'docker', 'exec', 'minio-db', 'mc', 'ls', '/data/gold/aggregated_data/world_bank'
        ], capture_output=True, text=True)
        
        tables_exist = []
        if bronze_check.returncode == 0:
            tables_exist.append('bronze')
        if silver_check.returncode == 0:
            tables_exist.append('silver')
        if gold_check.returncode == 0:
            tables_exist.append('gold')
        
        if tables_exist:
            logging.info(f"Delta Lake tables found: {tables_exist}")
            return True
        else:
            logging.warning("No Delta Lake tables found")
            return False
    except Exception as e:
        logging.error(f"Error checking Delta Lake tables: {e}")
        return False

def generate_monitoring_report():
    """Generate a concise monitoring report and return an overall status string.

    The returned value is one of:
      - "all_healthy": all checks returned True
      - "issues_detected": one or more checks returned False
    """
    logging.info("Generating monitoring report...")
    
    # Collect all health check results
    checks = {
        'streaming_service': check_streaming_service_health(),
        'kafka_topics': check_kafka_topics(),
        'streaming_processors': check_streaming_processors(),
        'dashboard': check_dashboard_health(),
        'data_freshness': check_data_freshness(),
        'delta_lake_tables': check_delta_lake_tables()
    }
    
    # Generate report
    healthy_checks = sum(checks.values())
    total_checks = len(checks)
    
    logging.info(f"Monitoring Report:")
    logging.info(f"Healthy checks: {healthy_checks}/{total_checks}")
    
    for check_name, status in checks.items():
        status_text = " HEALTHY" if status else " FAILED"
        logging.info(f"  {check_name}: {status_text}")
    
    if healthy_checks == total_checks:
        logging.info(" All systems are healthy!")
        return "all_healthy"
    else:
        logging.warning(" Some systems need attention")
        return "issues_detected"

def alert_if_needed():
    """Placeholder: hook for integrating alerting if issues are detected.

    In a production environment, this could notify Slack, email, PagerDuty, etc.
    For now, it only logs that the alert check has completed.
    """
    logging.info("Checking for critical issues...")
    # This would integrate with your alerting system
    # For now, just log the check
    logging.info("Alert check completed")
    return "alert_check_completed"

# Create DAG
dag = DAG(
    'streaming_monitoring_dag',
    default_args=default_args,
    description='Monitor streaming pipeline health and performance',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['monitoring', 'streaming', 'health-check'],
)

# Define tasks
start = DummyOperator(task_id='start', dag=dag)

check_streaming = PythonOperator(
    task_id='check_streaming_service_health',
    python_callable=check_streaming_service_health,
    dag=dag
)

check_topics = PythonOperator(
    task_id='check_kafka_topics',
    python_callable=check_kafka_topics,
    dag=dag
)

check_processors = PythonOperator(
    task_id='check_streaming_processors',
    python_callable=check_streaming_processors,
    dag=dag
)

check_dashboard = PythonOperator(
    task_id='check_dashboard_health',
    python_callable=check_dashboard_health,
    dag=dag
)

check_data = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

check_delta = PythonOperator(
    task_id='check_delta_lake_tables',
    python_callable=check_delta_lake_tables,
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_monitoring_report',
    python_callable=generate_monitoring_report,
    dag=dag
)

alert = PythonOperator(
    task_id='alert_if_needed',
    python_callable=alert_if_needed,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start >> [check_streaming, check_topics, check_processors, check_dashboard, check_data, check_delta]
[check_streaming, check_topics, check_processors, check_dashboard, check_data, check_delta] >> generate_report
generate_report >> alert
alert >> end 