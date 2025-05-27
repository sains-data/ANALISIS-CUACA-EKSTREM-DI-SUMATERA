#!/usr/bin/env python3
"""
Test script sederhana untuk Spark MLlib
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

def main():
    try:
        print("🚀 Initializing Spark Session...")
        
        spark = SparkSession.builder \
            .appName('TestSparkML') \
            .config('spark.executor.memory', '2g') \
            .config('spark.driver.memory', '1g') \
            .getOrCreate()
        
        print("✅ Spark Session initialized")
        
        # Test baca data dari Silver layer
        print("📊 Testing data loading...")
        
        df = spark.read \
            .option('header', 'true') \
            .option('inferSchema', 'true') \
            .csv('hdfs://namenode:9000/data/silver/weather_data_combined_sorted.csv')
        
        print(f"✅ Loaded {df.count()} rows")
        print("📋 Schema:")
        df.printSchema()
        
        print("📊 Sample data:")
        df.show(5)
        
        # Test basic feature engineering
        df_clean = df.filter(
            col('TANGGAL').isNotNull() &
            col('TN').isNotNull() & 
            col('TX').isNotNull() &
            col('TAVG').isNotNull()
        )
        
        print(f"📋 After cleaning: {df_clean.count()} rows")
        
        # Test simple aggregation
        monthly_stats = df_clean.groupBy(
            year(to_date(col('TANGGAL'), 'dd-MM-yyyy')).alias('year'),
            month(to_date(col('TANGGAL'), 'dd-MM-yyyy')).alias('month')
        ).agg(
            avg(col('TAVG').cast('double')).alias('avg_temp'),
            sum(col('RR').cast('double')).alias('total_rain'),
            count('*').alias('record_count')
        ).orderBy('year', 'month')
        
        print("📊 Monthly statistics:")
        monthly_stats.show()
        
        # Test save ke Gold layer
        print("💾 Testing save to Gold layer...")
        monthly_stats.coalesce(1).write.mode('overwrite').parquet(
            'hdfs://namenode:9000/data/gold/test_monthly_stats'
        )
        
        print("✅ Test completed successfully!")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
