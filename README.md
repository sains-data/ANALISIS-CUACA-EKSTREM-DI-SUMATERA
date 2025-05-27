# 🌦️ Weather Data Big Data Pipeline

Proyek ini adalah implementasi lengkap **Medallion Architecture** (Bronze → Silver → Gold) untuk memproses data cuaca menggunakan teknologi big data modern seperti Hadoop HDFS, Apache Spark, Hive, dan Airflow.

## 🏗️ Arsitektur Sistem

```
┌─────────────┐    ┌──────────┐    ┌─────────────┐    ┌──────────┐    ┌────────────┐
│ Data Source │ →  │  Bronze  │ →  │   Silver    │ →  │   Gold   │ →  │Visualization│
│ (Excel)     │    │  (HDFS)  │    │ (Processed) │    │(Analytics)│    │ (Superset) │
└─────────────┘    └──────────┘    └─────────────┘    └──────────┘    └────────────┘
                           ↑                ↑                ↑
                    ┌──────┴────────────────┴────────────────┴──────┐
                    │            AIRFLOW ORCHESTRATION             │
                    └───────────────────────────────────────────────┘
```

## 🛠️ Teknologi yang Digunakan

- **🐘 Hadoop HDFS** - Distributed File System
- **⚡ Apache Spark** - Data Processing Engine  
- **🏗️ Apache Hive** - Data Warehouse
- **🌀 Apache Airflow** - Workflow Orchestration
- **🐘 PostgreSQL** - Metadata Store
- **🐳 Docker** - Containerization

## 📋 Prasyarat

- Docker Desktop (versi terbaru)
- Docker Compose v2.x
- Minimal RAM: 8GB
- Disk space: 20GB
- Windows 10/11 atau Linux

## 📁 Struktur Project

```
project-bigdata/
├── 📄 docker-compose.yaml    # Orchestrasi container
├── 📄 hadoop.env            # Konfigurasi Hadoop
├── 📄 requirements.txt      # Dependencies Python
├── 📄 README.md             # Dokumentasi ini
├── 📁 config/               # Konfigurasi services
│   ├── 📁 hadoop/           # Config Hadoop
│   ├── 📁 spark/            # Config Spark
│   ├── 📁 hive/             # Config Hive
│   └── 📁 airflow/          # Config Airflow
├── 📁 data/                 # Data layers (Medallion)
│   ├── 📁 bronze/           # Raw data (Excel files)
│   ├── 📁 silver/           # Processed data (Parquet)
│   └── 📁 gold/             # Analytics data
├── 📁 scripts/              # Processing scripts
│   ├── 📄 spark_weather_processing.py    # Bronze → Silver
│   ├── 📄 copy_silver_to_local.py        # HDFS sync
│   ├── 📄 gold_layer_analytics.py        # Gold layer processing
│   ├── 📄 view_processed_data.py         # Data inspection
│   ├── 📄 check_excel.py                 # Data validation
│   ├── 📄 load_weather_data.hql          # Hive table setup
│   └── 📄 postgres-init.sql              # Database init
├── 📁 dags/                 # Airflow DAGs
│   └── 📄 weather_pipeline.py            # Main pipeline
└── 📄 weather_data_pipeline.ipynb        # Interactive notebook
├── 📁 scripts/              # Processing scripts
│   ├── 📄 spark_weather_processing.py
│   ├── 📄 gold_layer_analytics.py
│   ├── 📄 check_excel.py
│   └── 📄 view_processed_data.py
├── 📁 dags/                 # Airflow DAGs
│   └── 📄 weather_pipeline.py
├── 📄 weather_data_pipeline.ipynb  # 🎯 Jupyter Notebook (Recommended)
└── 📁 logs/                 # Log files
```

## 🎯 Quick Start Guide

### 🚀 **Option A: One-Command Pipeline (Recommended)**

**📟 Complete automation with single command**
```powershell
# 1. Start Docker services
docker-compose up -d

# 2. Run complete pipeline (Windows)
.\pipeline.ps1 full
# OR
pipeline.bat full

# 3. Check pipeline status
.\pipeline.ps1 status
```

**⚡ Individual Steps:**
```powershell
.\pipeline.ps1 check    # Validate data
.\pipeline.ps1 bronze   # Bronze → Silver
.\pipeline.ps1 sync     # HDFS → Local  
.\pipeline.ps1 view     # Inspect data
.\pipeline.ps1 gold     # Analytics
```

---

### 📓 **Option B: Interactive Jupyter Notebook**

**🎯 Comprehensive Pipeline in Single Notebook**
```powershell
# 1. Start Docker services
docker-compose up -d

# 2. Open weather_data_pipeline.ipynb
# 3. Run all cells step by step
```

**✨ Jupyter Notebook Features:**
- **🔄 Consolidated Workflow**: Complete pipeline in one notebook
- **📊 Interactive Execution**: Run steps individually with real-time feedback
- **🎯 Data Quality Analysis**: Built-in statistics and validation
- **🔧 Error Handling**: Clear troubleshooting guidance

---

### 🛠️ **Option C: Individual Scripts (Advanced)**

### **TAHAP 1: PERSIAPAN DOCKER & INFRASTRUKTUR**

#### 1.1 Verifikasi Docker
```powershell
# Pastikan Docker Desktop sudah running
docker --version
docker-compose --version
```

#### 1.2 Clone & Setup Project
```powershell
# Masuk ke direktori project
cd "d:\Coding\Python\src\ABD\project-bigdata"

# (Opsional) Bersihkan container lama
docker-compose down -v
docker system prune -f
```

#### 1.3 Start All Services
```powershell
# Jalankan semua container
docker-compose up -d

# Tunggu 2-3 menit hingga semua service ready
# Cek status container
docker-compose ps
```

#### 1.4 Verifikasi Web Interface
- **Hadoop NameNode**: http://localhost:9870
- **Spark Master**: http://localhost:8080  
- **Airflow**: http://localhost:8081 (admin/admin)

---

### **TAHAP 2: PERSIAPAN DATA SUMBER**

#### 2.1 Verifikasi Data Excel
```powershell
# Pastikan file Excel ada di folder bronze
ls data/bronze/
# Output: agustus_20.xlsx, agustus_21.xlsx, agustus_22.xlsx, agustus_23.xlsx
```

#### 2.2 Inspeksi Struktur Data
```powershell
# Jalankan script untuk melihat struktur data
python scripts/check_excel.py
```

---

### **TAHAP 3: SETUP HDFS STORAGE**

#### 3.1 Buat Struktur Folder HDFS
```powershell
# Hapus folder lama (jika ada)
docker exec namenode hdfs dfs -rm -r /data

# Buat struktur Medallion Architecture
docker exec namenode hdfs dfs -mkdir -p /data/bronze
docker exec namenode hdfs dfs -mkdir -p /data/silver
docker exec namenode hdfs dfs -mkdir -p /data/gold

# Set permission
docker exec namenode hdfs dfs -chmod -R 777 /data

# Verifikasi
docker exec namenode hdfs dfs -ls /data
```

---

### **TAHAP 4: UPLOAD DATA KE BRONZE LAYER**

#### 4.1 Copy Data ke Container
```powershell
# Copy folder bronze ke namenode container
docker cp data/bronze/. namenode:/tmp/bronze/

# Verifikasi file di container
docker exec namenode ls -la /tmp/bronze/
```

#### 4.2 Upload ke HDFS Bronze
```powershell
# Upload semua file Excel ke HDFS
docker exec namenode hdfs dfs -put /tmp/bronze/agustus_20.xlsx /data/bronze/
docker exec namenode hdfs dfs -put /tmp/bronze/agustus_21.xlsx /data/bronze/
docker exec namenode hdfs dfs -put /tmp/bronze/agustus_22.xlsx /data/bronze/
docker exec namenode hdfs dfs -put /tmp/bronze/agustus_23.xlsx /data/bronze/

# Verifikasi upload
docker exec namenode hdfs dfs -ls /data/bronze/
```

#### 4.3 Bulk Upload Method (Alternative)
```powershell
# Method 1: Bulk upload using wildcard (Recommended for multiple files)
# Step 1: Copy local bronze folder to namenode container
docker cp "data/bronze" namenode:/tmp/bronze

# Step 2: Create HDFS directory if not exists
docker exec namenode hdfs dfs -mkdir -p /data/bronze

# Step 3: Bulk upload all Excel files using wildcard
docker exec namenode sh -c "cd /tmp/bronze && hdfs dfs -put *.xlsx /data/bronze/"

# Verify upload
docker exec namenode hdfs dfs -ls /data/bronze/
```

**🔄 Comparison: CSV vs Excel Upload**
```powershell
# For CSV files (if you have them in bronze_csv folder)
docker cp "data/bronze_csv" namenode:/tmp/bronze_csv
docker exec namenode hdfs dfs -mkdir -p /data/bronze_csv
docker exec namenode sh -c "cd /tmp/bronze_csv && hdfs dfs -put *.csv /data/bronze_csv/"

# File size comparison:
# - Excel files (.xlsx): ~45-50 KB per file
# - CSV files (.csv): ~15-20 KB per file
```

**⚡ Quick Clean & Re-upload**
```powershell
# Clean existing data (if needed)
docker exec namenode hdfs dfs -rm -r /data/bronze/*
docker exec namenode rm -rf /tmp/bronze

# Fresh upload
docker cp "data/bronze" namenode:/tmp/bronze
docker exec namenode sh -c "cd /tmp/bronze && hdfs dfs -put *.xlsx /data/bronze/"
```

---

### **TAHAP 5: PROSES BRONZE → SILVER**

#### 5.1 Jalankan Spark Processing
```powershell
# Submit Spark job untuk memproses Excel ke Parquet
docker exec spark-master /spark/bin/spark-submit --packages com.crealytics:spark-excel_2.12:3.3.1_0.18.5 --master spark://spark-master:7077 --executor-memory 1g --total-executor-cores 2 /scripts/spark_weather_processing.py
```

#### 5.2 Verifikasi Silver Layer
```powershell
# Cek hasil di HDFS Silver
docker exec namenode hdfs dfs -ls /data/silver/

# Lihat data hasil processing
docker exec -it spark-master /spark/bin/spark-submit /scripts/view_processed_data.py

# Atau lihat data lokal (jika ada)
python read_silver_data.py
```

---

### **TAHAP 6: GOLD LAYER ANALYTICS (Opsional)**

#### 6.1 Generate Analytics
```powershell
# Jalankan script analytics untuk Gold layer
docker exec -it spark-master /spark/bin/spark-submit /scripts/gold_layer_analytics.py
```

#### 6.2 Setup Hive Tables
```powershell
# Load data ke Hive untuk query SQL
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/load_weather_data.hql
```

---

### **TAHAP 7: ORCHESTRATION DENGAN AIRFLOW**

#### 7.1 Akses Airflow Web UI
1. Buka http://localhost:8081
2. Login: `admin` / `admin`
3. Aktifkan DAG `weather_data_pipeline`

#### 7.2 Run Pipeline via Airflow
```powershell
# Atau trigger via CLI
docker exec airflow-scheduler airflow dags trigger weather_data_pipeline
```

## 🎯 Data Pipeline Flow

1. **Bronze Layer**: Raw Excel files di HDFS (`/data/bronze/`)
2. **Silver Layer**: Cleaned Parquet files di HDFS (`/data/silver/`)
3. **Gold Layer**: Aggregated analytics di HDFS (`/data/gold/`)
4. **Hive Tables**: SQL-queryable tables untuk BI tools
5. **Airflow**: Automated workflow orchestration

## 📊 Monitoring & Web Interface

| Service | URL | Credentials |
|---------|-----|-------------|
| Hadoop NameNode | http://localhost:9870 | - |
| Spark Master | http://localhost:8080 | - |
| Airflow | http://localhost:8081 | admin/admin |
| PostgreSQL | localhost:5432 | airflow/airflow |

## 🔍 Troubleshooting

### Container Issues
```powershell
# Cek logs container
docker logs namenode
docker logs spark-master
docker logs airflow-webserver

# Restart specific service
docker-compose restart namenode
```

### HDFS Upload Issues
```powershell
# Check if files were copied to container
docker exec namenode ls -la /tmp/bronze/

# Check HDFS Bronze directory
docker exec namenode hdfs dfs -ls /data/bronze/

# If upload fails, check HDFS space
docker exec namenode hdfs dfs -df /

# Re-upload specific file
docker exec namenode hdfs dfs -put /tmp/bronze/filename.xlsx /data/bronze/

# Bulk re-upload all files
docker exec namenode sh -c "cd /tmp/bronze && hdfs dfs -put *.xlsx /data/bronze/"
```

### HDFS Issues
```powershell
# Cek status HDFS
docker exec namenode hdfs dfsadmin -report

# Safe mode issues
docker exec namenode hdfs dfsadmin -safemode leave
```

### Spark Job Issues
```powershell
# Cek Spark logs
docker exec spark-master ls -la /spark/logs/

# Test Spark connectivity
docker exec spark-master /spark/bin/spark-shell --master spark://spark-master:7077
```

### Airflow Issues
```powershell
# Reset Airflow database
docker exec airflow-init airflow db reset

# Restart Airflow services
docker-compose restart airflow-webserver airflow-scheduler
```

## 🧹 Cleanup

```powershell
# Stop semua container
docker-compose down

# Hapus volumes dan data (HATI-HATI!)
docker-compose down -v

# Clean system
docker system prune -f
```

## 📚 Scripts & Files Reference

| File/Script | Fungsi | Usage |
|-------------|--------|-------|
| **🎯 pipeline.ps1** | **Complete pipeline automation** | `.\pipeline.ps1 full` |
| **📓 weather_data_pipeline.ipynb** | **Interactive notebook pipeline** | Open in Jupyter |
| `scripts/pipeline_manager.py` | Python pipeline orchestrator | `python scripts/pipeline_manager.py` |
| `scripts/check_excel.py` | Inspeksi struktur data Excel | `.\pipeline.ps1 check` |
| `scripts/spark_weather_processing.py` | Bronze → Silver processing | `.\pipeline.ps1 bronze` |
| `scripts/copy_silver_to_local.py` | HDFS → Local sync | `.\pipeline.ps1 sync` |
| `scripts/view_processed_data.py` | View Silver layer data | `.\pipeline.ps1 view` |
| `scripts/gold_layer_analytics.py` | Silver → Gold analytics | `.\pipeline.ps1 gold` |
| `scripts/load_weather_data.hql` | Setup Hive tables | Manual execution |
| `dags/weather_pipeline.py` | Airflow DAG pipeline | Via Airflow UI |

**💡 Recommendation:** 
- **Beginners**: Use `.\pipeline.ps1 full` for complete automation
- **Interactive**: Use `weather_data_pipeline.ipynb` for learning
- **Advanced**: Use individual scripts for custom workflows

## 🤝 Contributing

1. Fork repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## 📄 License

MIT License - see LICENSE file for details

---

**🎉 Happy Big Data Processing!** 

Untuk pertanyaan atau issues, silakan buat GitHub issue atau contact maintainer.
