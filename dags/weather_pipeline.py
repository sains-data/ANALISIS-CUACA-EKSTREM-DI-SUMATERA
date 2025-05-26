from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add project directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from convert_excel import convert_and_clean_excel

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

def convert_bronze_to_silver(**context):
    """Convert Excel files from HDFS bronze to local silver with data cleaning"""
    hdfs_bronze_path = '/data/bronze'
    local_silver_path = '/data/silver'
    successful, failed = convert_and_clean_excel(hdfs_bronze_path, local_silver_path)
    if failed > 0:
        raise Exception(f"Failed to convert {failed} files")
    return f"Successfully converted {successful} files"

def sync_silver_to_hdfs(**context):
    """Sync local silver layer to HDFS silver layer"""
    import subprocess
    try:
        cmd = """docker exec namenode bash -c '
            hdfs dfs -put -f /data/silver/* /data/silver/ &&
            echo "Silver layer synced to HDFS successfully"'"""
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Failed to sync silver layer: {result.stderr}")
        return "Silver layer synced successfully"
    except Exception as e:
        print(f"Error syncing silver layer: {str(e)}")
        raise

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

# Task 3: Convert bronze HDFS to silver local (Excel to CSV with cleaning)
convert_to_silver = PythonOperator(
    task_id='convert_bronze_to_silver',
    python_callable=convert_bronze_to_silver,
    dag=dag
)

# Task 4: Sync silver local to HDFS silver
sync_silver = PythonOperator(
    task_id='sync_silver_to_hdfs',
    python_callable=sync_silver_to_hdfs,
    dag=dag
)

# Set task dependencies
create_hdfs_dirs >> sync_bronze >> convert_to_silver >> sync_silver