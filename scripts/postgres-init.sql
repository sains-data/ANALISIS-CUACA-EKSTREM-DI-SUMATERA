-- Create databases
CREATE DATABASE hive_metastore;
CREATE DATABASE superset;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO airflow;
GRANT ALL PRIVILEGES ON DATABASE superset TO airflow;
