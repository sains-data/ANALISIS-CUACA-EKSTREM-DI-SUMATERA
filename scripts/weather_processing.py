from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def process_weather_data():
    # Inisialisasi Spark session
    spark = SparkSession.builder \
        .appName("Weather Data Processing") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .enableHiveSupport() \
        .getOrCreate()

    # Schema untuk data cuaca
    schema = StructType([
        StructField("TANGGAL", StringType(), True),
        StructField("TN", DoubleType(), True),
        StructField("TX", DoubleType(), True),
        StructField("TAVG", DoubleType(), True),
        StructField("RH_AVG", DoubleType(), True),
        StructField("RR", DoubleType(), True),
        StructField("SS", DoubleType(), True),
        StructField("FF_X", DoubleType(), True),
        StructField("DDD_X", DoubleType(), True),
        StructField("FF_AVG", DoubleType(), True),
        StructField("DDD_CAR", StringType(), True)
    ])

    # Baca data dari HDFS silver layer
    df = spark.read.csv("/data/silver/*", schema=schema, header=True)

    # Proses dan bersihkan data
    processed_df = df \
        .withColumn("date", to_date("TANGGAL", "yyyy-MM-dd")) \
        .withColumn("temp_min", col("TN")) \
        .withColumn("temp_max", col("TX")) \
        .withColumn("temp_avg", col("TAVG")) \
        .withColumn("humidity", col("RH_AVG")) \
        .withColumn("rainfall", col("RR")) \
        .withColumn("sunshine_duration", col("SS")) \
        .withColumn("wind_speed_max", col("FF_X")) \
        .withColumn("wind_direction_max", col("DDD_X")) \
        .withColumn("wind_speed_avg", col("FF_AVG")) \
        .withColumn("wind_direction", col("DDD_CAR")) \
        .withColumn("processing_date", current_date())

    # Simpan hasil ke HDFS gold layer dalam format Parquet
    processed_df.write \
        .mode("overwrite") \
        .partitionBy("processing_date") \
        .parquet("/data/gold/weather_data")

    # Tutup Spark session
    spark.stop()

if __name__ == "__main__":
    process_weather_data()
