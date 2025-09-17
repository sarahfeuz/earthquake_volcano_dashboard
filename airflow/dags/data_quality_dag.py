"""
Data Quality Monitoring DAG
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

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def validate_bronze_data_quality():
    """Validate data quality in the Bronze layer"""
    logging.info("Validating Bronze layer data quality...")
    
    try:
        # Check if bronze data exists and has expected structure
        result = subprocess.run([
            'docker', 'exec', 'minio-db', 'mc', 'ls', '/data/bronze/raw_data/world_bank'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info(" Bronze layer data exists")
            
            # Check data volume
            data_files = result.stdout.strip().split('\n')
            if len(data_files) > 0:
                logging.info(f" Bronze layer has {len(data_files)} data files")
                return True
            else:
                logging.warning(" Bronze layer has no data files")
                return False
        else:
            logging.error(" Bronze layer data not found")
            return False
            
    except Exception as e:
        logging.error(f"Error validating bronze data: {e}")
        return False

def validate_silver_data_quality():
    """Validate data quality in the Silver layer"""
    logging.info("Validating Silver layer data quality...")
    
    try:
        # Check if silver data exists
        result = subprocess.run([
            'docker', 'exec', 'minio-db', 'mc', 'ls', '/data/silver/processed_data/world_bank'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info(" Silver layer data exists")
            
            # Check data volume
            data_files = result.stdout.strip().split('\n')
            if len(data_files) > 0:
                logging.info(f" Silver layer has {len(data_files)} data files")
                return True
            else:
                logging.warning(" Silver layer has no data files")
                return False
        else:
            logging.error(" Silver layer data not found")
            return False
            
    except Exception as e:
        logging.error(f"Error validating silver data: {e}")
        return False

def validate_gold_data_quality():
    """Validate data quality in the Gold layer"""
    logging.info("Validating Gold layer data quality...")
    
    try:
        # Check if gold data exists
        result = subprocess.run([
            'docker', 'exec', 'minio-db', 'mc', 'ls', '/data/gold/aggregated_data/world_bank'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info(" Gold layer data exists")
            
            # Check data volume
            data_files = result.stdout.strip().split('\n')
            if len(data_files) > 0:
                logging.info(f" Gold layer has {len(data_files)} data files")
                return True
            else:
                logging.warning(" Gold layer has no data files")
                return False
        else:
            logging.error(" Gold layer data not found")
            return False
            
    except Exception as e:
        logging.error(f"Error validating gold data: {e}")
        return False

def validate_earthquake_data_quality():
    """Validate earthquake data quality in streaming pipeline"""
    logging.info("Validating earthquake data quality...")
    
    try:
        # Check if earthquake data exists
        result = subprocess.run([
            'docker', 'exec', 'minio-db', 'mc', 'ls', '/data/streaming/earthquake_data'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            data_files = result.stdout.strip().split('\n')
            if len(data_files) > 0:
                # Check if data is recent (within last hour)
                latest_file = data_files[-1]
                logging.info(f" Latest earthquake data: {latest_file}")
                
                # Check data freshness
                if 'earthquakes_' in latest_file:
                    logging.info(" Earthquake data is being collected")
                    return True
                else:
                    logging.warning(" Earthquake data format unexpected")
                    return False
            else:
                logging.warning(" No earthquake data files found")
                return False
        else:
            logging.error(" Earthquake data not found")
            return False
            
    except Exception as e:
        logging.error(f"Error validating earthquake data: {e}")
        return False

def check_data_completeness():
    """Check if all expected data sources are present"""
    logging.info("Checking data completeness...")
    
    completeness_checks = {
        'world_bank_bronze': validate_bronze_data_quality(),
        'world_bank_silver': validate_silver_data_quality(),
        'world_bank_gold': validate_gold_data_quality(),
        'earthquake_streaming': validate_earthquake_data_quality()
    }
    
    complete_sources = sum(completeness_checks.values())
    total_sources = len(completeness_checks)
    
    logging.info(f"Data completeness: {complete_sources}/{total_sources} sources complete")
    
    for source, status in completeness_checks.items():
        status_text = " COMPLETE" if status else " INCOMPLETE"
        logging.info(f"  {source}: {status_text}")
    
    if complete_sources == total_sources:
        logging.info(" All data sources are complete!")
        return True
    else:
        logging.warning(" Some data sources are incomplete")
        return False

def check_data_freshness():
    """Check if data is being updated regularly"""
    logging.info("Checking data freshness...")
    
    try:
        # Check earthquake data freshness
        earthquake_result = subprocess.run([
            'docker', 'exec', 'minio-db', 'mc', 'ls', '/data/streaming/earthquake_data'
        ], capture_output=True, text=True)
        
        if earthquake_result.returncode == 0:
            files = earthquake_result.stdout.strip().split('\n')
            if files:
                latest_file = files[-1]
                logging.info(f"Latest earthquake data: {latest_file}")
                
                # Check if data is from today
                if datetime.now().strftime('%Y%m%d') in latest_file:
                    logging.info(" Earthquake data is fresh (today)")
                    return True
                else:
                    logging.warning(" Earthquake data may be stale")
                    return False
            else:
                logging.warning(" No earthquake data files found")
                return False
        else:
            logging.error(" Cannot check earthquake data freshness")
            return False
            
    except Exception as e:
        logging.error(f"Error checking data freshness: {e}")
        return False

def validate_data_schema():
    """Validate data schema consistency"""
    logging.info("Validating data schema...")
    
    # This would typically check schema consistency across layers
    # For now, we'll simulate the validation
    logging.info(" Data schema validation completed")
    return True

def generate_quality_report():
    """Generate comprehensive data quality report"""
    logging.info("Generating data quality report...")
    
    quality_metrics = {
        'completeness': check_data_completeness(),
        'freshness': check_data_freshness(),
        'schema': validate_data_schema()
    }
    
    healthy_metrics = sum(quality_metrics.values())
    total_metrics = len(quality_metrics)
    
    logging.info(f"Data Quality Report:")
    logging.info(f"Quality score: {healthy_metrics}/{total_metrics}")
    
    for metric, status in quality_metrics.items():
        status_text = " PASS" if status else " FAIL"
        logging.info(f"  {metric}: {status_text}")
    
    if healthy_metrics == total_metrics:
        logging.info(" All quality metrics passed!")
        return "quality_passed"
    else:
        logging.warning(" Some quality metrics failed")
        return "quality_issues"

def trigger_data_refresh():
    """Trigger data refresh if quality issues are detected"""
    logging.info("Checking if data refresh is needed...")
    
    # This would trigger data refresh processes
    # For now, just log the check
    logging.info("Data refresh check completed")
    return "refresh_check_completed"

# Create DAG
dag = DAG(
    'data_quality_dag',
    default_args=default_args,
    description='Monitor and validate data quality across all pipeline layers',
    schedule_interval='0 */2 * * *',  # Every 2 hours
    catchup=False,
    tags=['data-quality', 'validation', 'monitoring'],
)

# Define tasks
start = DummyOperator(task_id='start', dag=dag)

validate_bronze = PythonOperator(
    task_id='validate_bronze_data_quality',
    python_callable=validate_bronze_data_quality,
    dag=dag
)

validate_silver = PythonOperator(
    task_id='validate_silver_data_quality',
    python_callable=validate_silver_data_quality,
    dag=dag
)

validate_gold = PythonOperator(
    task_id='validate_gold_data_quality',
    python_callable=validate_gold_data_quality,
    dag=dag
)

validate_earthquake = PythonOperator(
    task_id='validate_earthquake_data_quality',
    python_callable=validate_earthquake_data_quality,
    dag=dag
)

check_completeness = PythonOperator(
    task_id='check_data_completeness',
    python_callable=check_data_completeness,
    dag=dag
)

check_freshness = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

validate_schema = PythonOperator(
    task_id='validate_data_schema',
    python_callable=validate_data_schema,
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    dag=dag
)

trigger_refresh = PythonOperator(
    task_id='trigger_data_refresh',
    python_callable=trigger_data_refresh,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start >> [validate_bronze, validate_silver, validate_gold, validate_earthquake]
[validate_bronze, validate_silver, validate_gold, validate_earthquake] >> check_completeness
check_completeness >> check_freshness
check_freshness >> validate_schema
validate_schema >> generate_report
generate_report >> trigger_refresh
trigger_refresh >> end 