# Project Big Data Pipeline

Project ini merupakan implementasi pipeline data untuk memproses data cuaca menggunakan berbagai teknologi big data seperti Hadoop, Spark, Hive, Airflow, dan Superset.

## Arsitektur Sistem

```
[Data Source] -> [HDFS] -> [Spark Processing] -> [Hive] -> [Superset]
     ↑             ↑            ↑                   ↑          ↑
     └──────────────────── [Airflow Orchestration] ───────────┘
```

## Prasyarat

- Docker dan Docker Compose
- Minimal RAM 8GB
- Disk space 20GB

## Struktur Project

```
project-bigdata/
├── config/          # Konfigurasi services
├── dags/            # Airflow DAGs
├── data/            # Data layers
├── logs/            # Log files
└── scripts/         # Processing scripts
```

## Cara Menjalankan

### 1. Menjalankan Services

```powershell
# Start semua container
docker-compose up -d

# Periksa status container
docker-compose ps
```

### 2. Mengakses Web Interface

- Hadoop NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- Airflow: http://localhost:8081 
  - Username: admin
  - Password: admin
- Apache Superset: http://localhost:8088
  - Username: admin
  - Password: admin

### 3. Pipeline Data

#### 3.1 Import Data ke HDFS

```powershell
# Copy data ke container namenode
docker cp ./data/raw/weather_data.csv namenode:/tmp/

# Buat direktori di HDFS
docker exec -it namenode hdfs dfs -mkdir -p /data/raw/

# Upload file ke HDFS
docker exec -it namenode hdfs dfs -put /tmp/weather_data.csv /data/raw/

# Verifikasi file
docker exec -it namenode hdfs dfs -ls /data/raw/
```

#### 3.2 Proses Data dengan Spark

```powershell
# Copy script dan konfigurasi ke Spark master
docker cp ./scripts/weather_processing.py spark-master:/opt/
docker cp ./config/hadoop/core-site.xml spark-master:/spark/conf/
docker cp ./config/hadoop/hdfs-site.xml spark-master:/spark/conf/

# Jalankan Spark job
docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/weather_processing.py
```

#### 3.3 Load Data ke Hive

```powershell
# Copy script HQL ke Hive server
docker cp ./scripts/load_weather_data.hql hive-server:/tmp/

# Jalankan script Hive
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /tmp/load_weather_data.hql
```

### 4. Konfigurasi Airflow

1. Buka Airflow UI di http://localhost:8081
2. Login dengan admin/admin
3. Buka menu "Admin" -> "Connections"
4. Tambahkan koneksi untuk Spark:
   - Conn Id: spark_default
   - Conn Type: Spark
   - Host: spark://spark-master
   - Port: 7077

5. Tambahkan koneksi untuk Hive:
   - Conn Id: hive_default
   - Conn Type: Hive Server 2
   - Host: hive-server
   - Port: 10000
   - Login: hive
   - Password: hive

### 5. Konfigurasi Superset

1. Buka Superset UI di http://localhost:8088
2. Login dengan admin/admin
3. Tambahkan database connection:
   - Database Type: PostgreSQL
   - Host: postgres
   - Port: 5432
   - Database Name: superset
   - Username: airflow
   - Password: airflow

## Data Pipeline Flow

1. Data raw (`weather_data.csv`) di-upload ke HDFS
2. Spark job memproses data dan menyimpan hasilnya dalam format Parquet
3. Data diload ke Hive untuk analisis
4. Airflow mengorkestrasikan seluruh proses
5. Data dapat divisualisasikan menggunakan Superset

## Monitoring

- Hadoop UI: Monitor status HDFS
- Spark UI: Monitor job processing
- Airflow UI: Monitor DAG execution
- Superset: Visualisasi data

## Troubleshooting

### 1. Container tidak berjalan
```powershell
# Periksa logs container
docker logs [container_name]
```

### 2. HDFS issues
```powershell
# Periksa status HDFS
docker exec -it namenode hdfs dfsadmin -report
```

### 3. Spark job gagal
```powershell
# Periksa Spark logs
docker exec -it spark-master ls -l /spark/logs/
```

### 4. Hive connection issues
```powershell
# Test Hive connection
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
```

## Pembersihan

```powershell
# Menghentikan semua container
docker-compose down

# Menghapus volume (optional)
docker-compose down -v
```
