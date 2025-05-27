from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add project directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.spark_weather_processing import process_excel_files

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Helper Functions
def ensure_hdfs_directories():
    """Create HDFS directories for all layers if they don't exist"""
    import subprocess
    try:
        cmd = """docker exec namenode bash -c '
            hdfs dfs -mkdir -p /data/bronze &&
            hdfs dfs -mkdir -p /data/silver &&
            hdfs dfs -mkdir -p /data/gold &&
            hdfs dfs -chmod -R 777 /data'"""
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Failed to create HDFS directories: {result.stderr}")
        return "HDFS directories created successfully"
    except Exception as e:
        print(f"Error creating HDFS directories: {str(e)}")
        raise

def sync_local_bronze_to_hdfs(**context):
    """Sync local bronze layer to HDFS bronze layer"""
    import subprocess
    try:
        cmd = """docker exec namenode bash -c '
            hdfs dfs -put -f /data/bronze/* /data/bronze/ && 
            echo "Bronze layer synced to HDFS successfully"'"""
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Failed to sync bronze layer: {result.stderr}")
        return "Bronze layer synced successfully"
    except Exception as e:
        print(f"Error syncing bronze layer: {str(e)}")
        raise

def process_with_spark(**context):
    """Process Excel files from HDFS bronze to silver using Spark"""
    from pyspark.sql import SparkSession
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("WeatherDataProcessing") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .getOrCreate()
    
    # Process files
    bronze_path = "/data/bronze"
    silver_path = "/data/silver"
    success = process_excel_files(spark, bronze_path, silver_path)
    
    if not success:
        raise Exception("Failed to process Excel files with Spark")
    return "Successfully processed files with Spark"

# Define DAG
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Pipeline for processing weather data using medallion architecture',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Task 1: Ensure HDFS directories exist
create_hdfs_dirs = PythonOperator(
    task_id='create_hdfs_directories',
    python_callable=ensure_hdfs_directories,
    dag=dag
)

# Task 2: Sync local bronze to HDFS bronze
sync_bronze = PythonOperator(
    task_id='sync_local_bronze_to_hdfs',
    python_callable=sync_local_bronze_to_hdfs,
    dag=dag
)

# Task 3: Process data with Spark
process_data = PythonOperator(
    task_id='process_with_spark',
    python_callable=process_with_spark,
    dag=dag
)

# Set task dependencies
create_hdfs_dirs >> sync_bronze >> process_data