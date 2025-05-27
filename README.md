# 🌦️ ANALISIS-CUACA-EKSTREM-DI-SUMATERA : Bandar Lampung

[![Docker](https://img.shields.io/badge/Docker-20.10+-blue.svg)](https://www.docker.com/)
[![Hadoop](https://img.shields.io/badge/Hadoop-3.3.4-yellow.svg)](https://hadoop.apache.org/)
[![Spark](https://img.shields.io/badge/Spark-3.3.0-orange.svg)](https://spark.apache.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.5.0-green.svg)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue.svg)](https://www.postgresql.org/)

## 📋 Project Overview

Sistem big data pipeline untuk analisis data cuaca menggunakan **Medallion Architecture** (Bronze → Silver → Gold) dengan teknologi modern untuk ETL, storage, dan analytics. Project ini memproses 52 file Excel data cuaca dari 2020-2023 menjadi insights yang dapat digunakan untuk analisis dan prediksi cuaca.

### 🎯 Key Features
- **Bronze Layer**: 52 Excel files (4 years weather data) stored in HDFS
- **Silver Layer**: Cleaned and processed CSV data 
- **Gold Layer**: Advanced analytics and ML predictions
- **Real-time Processing**: Spark-based ETL pipeline
- **Data Orchestration**: Apache Airflow workflow management
- **Visualization**: Apache Superset dashboard
- **Machine Learning**: Random Forest weather prediction model

---

## 🏗️ Technology Stack

### **Core Big Data Infrastructure**
- **🐳 Docker & Docker Compose**: Complete containerization
- **🐘 Hadoop HDFS**: Distributed file system (NameNode + DataNode)
- **⚡ Apache Spark**: Big data processing (Master + Worker)
- **🌊 Apache Airflow**: Workflow orchestration & scheduling
- **🐘 PostgreSQL**: Metadata store & data warehouse
- **📊 Apache Superset**: Business intelligence & visualization
- **🔄 Redis**: Caching and message broker

### **Data Processing & Analytics**
- **Python 3.8+**: Primary programming language
- **PySpark**: Spark Python API for big data processing
- **Pandas & NumPy**: Data manipulation and analysis
- **Scikit-learn**: Machine learning algorithms
- **Apache Hive**: SQL-like data warehouse queries

---

## 📁 Complete Project Structure

```
d:\Coding\Python\src\ABD\project-bigdata/
├── 📋 docker-compose.yaml          # Main orchestration (9 services)
├── 📋 hadoop.env                   # Hadoop environment variables
├── 📋 requirements.txt             # Python dependencies
├── 📋 README.md                    # Main documentation
├── 📋 README_COMPLETE.md           # This comprehensive guide
│
├── 🔧 config/                      # Service configurations
│   ├── airflow/
│   │   ├── airflow.cfg            # Airflow configuration
│   │   └── config.py              # Custom Airflow settings
│   ├── hadoop/
│   │   ├── core-site.xml          # Hadoop core configuration
│   │   └── hdfs-site.xml          # HDFS configuration
│   ├── hive/
│   │   └── hive-site.xml          # Hive warehouse configuration
│   ├── jupyter/
│   │   └── jupyter_notebook_config.py  # Jupyter settings
│   ├── spark/
│   │   └── spark-defaults.conf    # Spark default configurations
│   └── superset/
│       ├── superset_config.py     # Superset configuration
│       └── __pycache__/
│
├── 📊 data/ (Medallion Architecture)
│   ├── bronze/                    # RAW DATA (52 Excel files)
│   │   ├── jan_20.xlsx → desember_23.xlsx  # 48 monthly files
│   │   └── [Additional 4 files]           # Total: 52 files
│   ├── silver/                    # PROCESSED DATA
│   │   └── weather_data_processed_final.csv
│   └── gold/                      # ANALYTICS & ML RESULTS
│       └── [Generated analytics files]
│
├── 🚀 scripts/                    # ETL & Processing Scripts
│   ├── hdfs_bronze_to_silver.py  # MAIN: HDFS Bronze→Silver ETL
│   ├── bronze_to_silver_final.py # Alternative Bronze→Silver
│   ├── simple_bronze_to_silver.py # Simplified processing
│   ├── spark_ml_weather_prediction.py # ML Random Forest model
│   ├── upload_interpolated_to_hdfs.py # Data upload utilities
│   ├── weather_analytics_comprehensive.hql # Hive analytics
│   ├── postgres-init.sql          # Database initialization
│   └── temp_excel/                # Temporary processing folder
│
├── 🔄 dags/                       # Airflow DAGs
│   ├── weather_pipeline.py       # Main pipeline orchestration
│   ├── convert_excel.py          # Excel conversion workflow
│   └── __pycache__/               # Compiled Python files
│
└── 📝 logs/                       # Application logs
    ├── dag_processor_manager/
    │   └── dag_processor_manager.log
    └── scheduler/
        ├── 2025-05-26/
        └── 2025-05-27/
            └── weather_pipeline.py.log
```

---

## 🐳 Docker Container Architecture

### **Container Services (9 containers)**

| Container | Image | Purpose | Ports | Dependencies |
|-----------|-------|---------|-------|--------------|
| **namenode** | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 | HDFS Name Node | 9000, 9870 | - |
| **datanode** | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8 | HDFS Data Node | 9864 | namenode |
| **spark-master** | bitnami/spark:3.3.0 | Spark Master Node | 7077, 8080 | - |
| **spark-worker** | bitnami/spark:3.3.0 | Spark Worker Node | 8081 | spark-master |
| **postgres** | postgres:13 | PostgreSQL Database | 5432 | - |
| **airflow-webserver** | apache/airflow:2.5.0 | Airflow Web UI | 8081 | postgres |
| **airflow-scheduler** | apache/airflow:2.5.0 | Airflow Scheduler | - | postgres |
| **superset** | apache/superset:latest | Superset BI Tool | 8088 | postgres |
| **superset-redis** | redis:latest | Redis Cache | 6379 | - |

---

## 🚀 Quick Start Guide

### **Prerequisites**
- ✅ Docker Desktop 4.0+ installed and running
- ✅ Docker Compose v2.0+
- ✅ Python 3.8+ with pip
- ✅ At least 8GB RAM available
- ✅ 15GB free disk space
- ✅ Windows 10/11 or Linux

### **Step 1: Environment Setup**

```powershell
# Navigate to project directory
cd "d:\Coding\Python\src\ABD\project-bigdata"

# Verify Docker installation
docker --version
docker-compose --version

# Check available system resources
docker system df
```

### **Step 2: Start Infrastructure**

```powershell
# Start all services (9 containers)
docker-compose up -d

# Monitor startup progress (wait 3-5 minutes)
docker-compose ps

# Verify all containers are healthy
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### **Step 3: Verify Service Access**

| Service | URL | Credentials | Status Check |
|---------|-----|-------------|--------------|
| **Hadoop NameNode** | http://localhost:9870 | None | HDFS Web UI |
| **Spark Master** | http://localhost:8080 | None | Spark Cluster Monitor |
| **Airflow WebUI** | http://localhost:8081 | admin/admin | DAG Management |
| **Superset** | http://localhost:8088 | admin/admin | BI Dashboard |
| **PostgreSQL** | localhost:5432 | airflow/airflow | Database Access |

---

## 🏃‍♂️ Data Pipeline Execution

### **🎯 Method 1: Main HDFS Pipeline (Recommended)**

This is the PRIMARY script that processes data directly from HDFS Bronze → Silver:

```powershell
# Execute main Bronze → Silver processing
python scripts/hdfs_bronze_to_silver.py
```

**What this script does:**
1. 📥 **HDFS Download**: Downloads 52 Excel files from HDFS Bronze layer
2. 🧹 **Data Cleaning**: Removes invalid values (8888, 9999, -, 0)
3. 🔄 **Data Processing**: Combines and normalizes weather data
4. 💾 **HDFS Upload**: Saves processed CSV to HDFS Silver layer
5. 📊 **Local Backup**: Creates local copy for verification

### **🔄 Method 2: Alternative Processing Scripts**

```powershell
# Alternative Bronze → Silver processing
python scripts/bronze_to_silver_final.py

# Simplified processing (no interpolation)
python scripts/simple_bronze_to_silver.py

# Upload processed data to HDFS
python scripts/upload_interpolated_to_hdfs.py
```

### **🤖 Method 3: Machine Learning Predictions**

```powershell
# Run Random Forest weather prediction model
python scripts/spark_ml_weather_prediction.py
```

**ML Pipeline Features:**
- ✅ Random Forest algorithm for weather prediction
- ✅ Feature engineering (temperature ranges, seasonal patterns)
- ✅ 7-day weather forecasting
- ✅ Model performance evaluation
- ✅ Feature importance analysis

---

## 📊 Data Processing Details

### **Bronze Layer (Raw Data)**
- **Location**: `/data/bronze/` in HDFS + Local `data/bronze/`
- **Format**: Excel files (.xlsx)
- **Count**: 52 files
- **Coverage**: January 2020 - December 2023 (4 years)
- **File Pattern**: `{month}_{year}.xlsx` (e.g., jan_20.xlsx, desember_23.xlsx)
- **Size**: ~15MB total

**Sample files:**
```
jan_20.xlsx, feb_20.xlsx, maret_20.xlsx, april_20.xlsx,
mei_20.xlsx, juni_20.xlsx, juli_20.xlsx, agustus_20.xlsx,
september_20.xlsx, oktober_20.xlsx, november_20.xlsx, desember_20.xlsx
... (continuing through 2021, 2022, 2023)
```

### **Silver Layer (Processed Data)**
- **Location**: `/data/silver/` in HDFS + Local `data/silver/`
- **Format**: CSV files
- **Main Output**: `weather_data_processed_final.csv`
- **Processing Applied**:
  - Invalid value cleaning (8888, 9999, -, 0 → NaN)
  - Data type conversion and validation
  - Missing value handling
  - Date standardization
  - Quality checks and reporting

### **Gold Layer (Analytics & ML)**
- **Location**: Local `data/gold/` + PostgreSQL database
- **Content**:
  - Monthly weather summaries
  - Temperature distribution analysis
  - Rainfall pattern insights
  - Extreme weather event detection
  - Random Forest ML predictions
  - Feature importance rankings

---

## 🔧 Essential Docker Commands

### **Container Management**

```powershell
# View all container status
docker-compose ps

# View real-time container logs
docker-compose logs -f namenode
docker-compose logs -f spark-master
docker-compose logs -f airflow-webserver

# Restart specific services
docker-compose restart namenode
docker-compose restart spark-master

# Scale Spark workers (if needed)
docker-compose up -d --scale spark-worker=2

# Stop all services
docker-compose down

# Complete cleanup (removes volumes and data)
docker-compose down -v
docker system prune -f
```

### **HDFS Operations**

```powershell
# List HDFS root directories
docker exec namenode hdfs dfs -ls /

# Check Bronze layer (should show 52 Excel files)
docker exec namenode hdfs dfs -ls /data/bronze/

# Check Silver layer (processed data)
docker exec namenode hdfs dfs -ls /data/silver/

# HDFS storage report
docker exec namenode hdfs dfsadmin -report

# Download file from HDFS to container
docker exec namenode hdfs dfs -get /data/silver/weather_data_processed_final.csv /tmp/

# Upload file to HDFS
docker exec namenode hdfs dfs -put /local/file.csv /data/bronze/

# Create HDFS directories
docker exec namenode hdfs dfs -mkdir -p /data/gold
```

### **Spark Operations**

```powershell
# Check Spark cluster status
# Visit: http://localhost:8080

# Submit Spark job with Python script
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  --driver-memory 1g \
  /path/to/script.py

# Test Spark connectivity
docker exec spark-master /opt/bitnami/spark/bin/spark-shell --version

# Monitor Spark applications via Web UI
start http://localhost:8080
```

---

## 📈 Expected Results & Outputs

### **Pipeline Success Indicators**
- ✅ 52 Excel files successfully processed from Bronze layer
- ✅ CSV file created in Silver layer (~1500+ weather records)
- ✅ ML model trained with accuracy metrics
- ✅ Analytics results generated in Gold layer
- ✅ All web interfaces accessible

### **Data Metrics**
- **Input Volume**: 52 Excel files (~15MB total)
- **Output Volume**: 1 processed CSV file (~2-3MB)
- **Record Count**: 1500-2000 weather observations
- **Processing Time**: 5-10 minutes for complete pipeline
- **Data Quality**: 95%+ clean data after processing

### **Generated Files**

**Silver Layer Output:**
```
📁 data/silver/
└── weather_data_processed_final.csv  # Main processed dataset
```

**Gold Layer Output:**
```
📁 data/gold/
├── weather_analytics_monthly_summary.csv
├── weather_analytics_extreme_analysis.csv
├── weather_analytics_weekly_trends.csv
├── weather_predictions_7_days.csv
├── feature_importance.csv
└── model_performance_metrics.csv
```

---

## 🎯 Airflow DAG Workflows

### **Available DAGs**

1. **`weather_pipeline.py`** - Main orchestration DAG
   - Bronze → Silver → Gold processing
   - Scheduled execution
   - Error handling and retries

2. **`convert_excel.py`** - Excel conversion workflow
   - Batch Excel processing
   - Data validation
   - Quality checks

### **DAG Management**

```powershell
# Access Airflow Web UI
start http://localhost:8081
# Login: admin/admin

# Command line DAG operations
docker exec airflow-scheduler airflow dags list
docker exec airflow-scheduler airflow tasks list weather_pipeline
docker exec airflow-scheduler airflow dags trigger weather_pipeline
```

---

## 🔍 Monitoring & Troubleshooting

### **Health Checks**

```powershell
# Check all containers health status
docker ps --filter "health=healthy"

# Verify HDFS accessibility
docker exec namenode hdfs dfsadmin -safemode get

# Test Spark connectivity
docker exec spark-master curl -s http://localhost:8080 | grep "Spark Master"

# Check Airflow scheduler status
docker exec airflow-scheduler airflow jobs check

# PostgreSQL connection test
docker exec postgres psql -U airflow -d airflow -c "SELECT version();"
```

### **Common Issues & Solutions**

#### **🔴 Container Startup Issues**
```powershell
# Check logs for startup errors
docker-compose logs namenode
docker-compose logs spark-master

# Restart with fresh state
docker-compose down -v
docker-compose up -d

# Wait for complete initialization
timeout /t 300  # Wait 5 minutes
```

#### **🔴 HDFS Connection Problems**
```powershell
# Verify namenode is out of safe mode
docker exec namenode hdfs dfsadmin -safemode get

# Leave safe mode if stuck
docker exec namenode hdfs dfsadmin -safemode leave

# Check HDFS web UI
start http://localhost:9870
```

#### **🔴 Spark Job Failures**
```powershell
# Check Spark cluster resources
docker exec spark-master curl http://localhost:8080/api/v1/applications

# View Spark logs
docker-compose logs spark-master
docker-compose logs spark-worker

# Restart Spark services
docker-compose restart spark-master spark-worker
```

#### **🔴 Memory Issues**
```powershell
# Check Docker resource usage
docker stats

# Increase Docker Desktop memory allocation
# Settings → Resources → Advanced → Memory: 8GB+

# Clean up unused resources
docker system prune -f
docker volume prune -f
```

---

## 🚀 Advanced Usage & Analytics

### **Custom Hive Analytics**

```powershell
# Connect to Hive and run analytics queries
docker exec namenode beeline -u jdbc:hive2://namenode:10000

# Run comprehensive analytics
docker exec namenode hive -f /path/to/weather_analytics_comprehensive.hql
```

### **PostgreSQL Direct Analytics**

```sql
-- Connect to PostgreSQL
psql -h localhost -p 5432 -U airflow -d airflow

-- Weather analytics queries
SELECT 
    DATE_TRUNC('month', tanggal) as month,
    AVG(tavg) as avg_temperature,
    SUM(rr) as total_rainfall,
    COUNT(*) as records
FROM weather_data 
GROUP BY DATE_TRUNC('month', tanggal)
ORDER BY month;

-- Extreme weather detection
SELECT 
    tanggal,
    tavg,
    rr,
    CASE 
        WHEN tavg > 35 THEN 'Hot'
        WHEN tavg < 20 THEN 'Cold'
        WHEN rr > 50 THEN 'Heavy Rain'
        ELSE 'Normal'
    END as weather_condition
FROM weather_data
WHERE tavg > 35 OR tavg < 20 OR rr > 50
ORDER BY tanggal;
```

### **Custom Spark Analytics**

```python
# Example: Custom Spark analytics script
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CustomWeatherAnalytics") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read from HDFS Silver layer
df = spark.read.csv("hdfs://namenode:9000/data/silver/weather_data_processed_final.csv", 
                    header=True, inferSchema=True)

# Custom analytics
monthly_stats = df.groupBy("month").agg(
    {"tavg": "avg", "rr": "sum", "*": "count"}
).orderBy("month")

monthly_stats.show()
spark.stop()
```

---

## 📊 Superset Dashboard Setup

### **Dashboard Configuration**

1. **Access Superset**: http://localhost:8088 (admin/admin)
2. **Add PostgreSQL Connection**:
   - Database URI: `postgresql://airflow:airflow@postgres:5432/airflow`
   - Test connection
3. **Create Datasets** from weather_data table
4. **Build Charts**: Temperature trends, rainfall patterns, seasonal analysis
5. **Create Dashboard**: Combine multiple charts

### **Sample Dashboard Elements**
- 📈 Temperature trend line chart
- 🌧️ Monthly rainfall bar chart
- 📊 Seasonal temperature distribution
- 🎯 Extreme weather alerts
- 📋 Data quality metrics

---

## 🔄 Maintenance & Updates

### **Regular Maintenance**

```powershell
# Weekly maintenance routine
docker-compose down
docker system prune -f
docker-compose up -d

# Log rotation
docker-compose logs --tail=100 > logs/weekly_$(Get-Date -Format 'yyyy-MM-dd').log

# HDFS health check
docker exec namenode hdfs fsck /data -summary
```

### **Data Backup**

```powershell
# Backup HDFS data
docker exec namenode hdfs dfs -get /data ./backup/hdfs_backup_$(Get-Date -Format 'yyyy-MM-dd')

# Backup PostgreSQL database
docker exec postgres pg_dump -U airflow airflow > backup/postgres_backup_$(Get-Date -Format 'yyyy-MM-dd').sql
```

---

## 📚 Additional Resources

### **Documentation Files**
- `README.md` - Main project documentation
- `PIPELINE_COMPLETION_SUMMARY.md` - Pipeline execution summary
- `config/` - Service configuration references
- `logs/` - Application and DAG execution logs

### **Script References**
- `scripts/hdfs_bronze_to_silver.py` - **PRIMARY** ETL script
- `scripts/spark_ml_weather_prediction.py` - ML prediction model
- `scripts/weather_analytics_comprehensive.hql` - Hive analytics
- `dags/weather_pipeline.py` - Airflow orchestration

---

## 🙏 Acknowledgments

- **Apache Software Foundation**: Hadoop, Spark, Airflow, Superset
- **Docker Inc**: Containerization platform
- **Bitnami**: Pre-configured Docker images
- **Weather Data Providers**: Sample datasets for analysis

---


## 🎉 Conclusion
This project successfully implements a complete big data pipeline for weather data processing using modern technologies and best practices. The Medallion Architecture ensures data quality and scalability, while the integration of machine learning prepares the dataset for advanced analytics.
The system is now ready for production use, providing valuable insights into weather patterns and enabling predictive analytics.    



---

**🎉 Happy Big Data Processing!**

For questions, issues, or contributions, please create a GitHub issue or contact the project maintainers.
