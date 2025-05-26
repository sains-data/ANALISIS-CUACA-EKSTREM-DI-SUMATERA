from airflow.configuration import conf

# MySQL connection
AIRFLOW__CORE__SQL_ALCHEMY_CONN = 'mysql+mysqlconnector://airflow:airflow@mysql:3306/airflow'

# DAG settings
AIRFLOW__CORE__LOAD_EXAMPLES = False
AIRFLOW__CORE__DAGS_FOLDER = '/opt/airflow/dags'

# Executor settings
AIRFLOW__CORE__EXECUTOR = 'LocalExecutor'

# Logging settings
AIRFLOW__LOGGING__BASE_LOG_FOLDER = '/opt/airflow/logs'
AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION = '/opt/airflow/logs/dag_processor_manager/dag_processor_manager.log'

# Web server settings
AIRFLOW__WEBSERVER__BASE_URL = 'http://localhost:8081'
AIRFLOW__WEBSERVER__EXPOSE_CONFIG = False

# Security settings
AIRFLOW__WEBSERVER__SECRET_KEY = 'your-secret-key-here'
AIRFLOW__CORE__SECURE_MODE = True
