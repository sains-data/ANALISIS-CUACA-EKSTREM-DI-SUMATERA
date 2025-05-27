#!/usr/bin/env python3
"""
Spark MLlib Random Forest Weather Prediction
Membaca data dari Silver layer, melakukan feature engineering, 
training Random Forest model, dan menyimpan prediksi ke Gold layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import sys

def initialize_spark():
    """Initialize Spark Session dengan MLlib"""
    print("🚀 Initializing Spark Session with MLlib...")
    
    spark = SparkSession.builder \
        .appName('WeatherPredictionRandomForest') \
        .config('spark.sql.adaptive.enabled', 'true') \
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \
        .config('spark.executor.memory', '4g') \
        .config('spark.driver.memory', '2g') \
        .config('spark.executor.cores', '2') \
        .getOrCreate()
    
    print("✅ Spark Session with MLlib initialized")
    return spark

def load_and_prepare_data(spark):
    """Load data dari Silver layer dan prepare untuk ML"""
    print("📊 Loading data from Silver layer...")
    
    try:
        # Baca data CSV dari Silver layer
        df = spark.read \
            .option('header', 'true') \
            .option('inferSchema', 'true') \
            .csv('hdfs://namenode:9000/data/silver/weather_data_combined_sorted.csv')
        
        print(f"✅ Loaded {df.count()} rows from Silver layer")
        
        # Bersihkan data dan tambah features
        df_clean = df.filter(
            col('TANGGAL').isNotNull() &
            col('TN').isNotNull() & 
            col('TX').isNotNull() &
            col('TAVG').isNotNull() &
            col('RH_AVG').isNotNull() &
            col('RR').isNotNull()
        )
        
        print(f"📋 After cleaning: {df_clean.count()} rows")
        
        # Feature engineering
        df_features = df_clean.select(
            # Konversi tanggal
            to_date(col('TANGGAL'), 'dd-MM-yyyy').alias('date'),
            col('TN').cast('double').alias('temp_min'),
            col('TX').cast('double').alias('temp_max'), 
            col('TAVG').cast('double').alias('temp_avg'),
            col('RH_AVG').cast('double').alias('humidity'),
            col('RR').cast('double').alias('rainfall'),
            col('SS').cast('double').alias('sunshine'),
            col('FF_AVG').cast('double').alias('wind_speed'),
            col('DDD_CAR').alias('wind_direction'),
            col('source_file')
        ).filter(col('date').isNotNull())
        
        # Tambah feature temporal
        df_with_temporal = df_features.withColumn('year', year(col('date'))) \
            .withColumn('month', month(col('date'))) \
            .withColumn('day_of_year', dayofyear(col('date'))) \
            .withColumn('quarter', quarter(col('date')))
        
        # Tambah feature derived
        df_engineered = df_with_temporal \
            .withColumn('temp_range', col('temp_max') - col('temp_min')) \
            .withColumn('heat_index', 
                       when(col('temp_avg') > 27, 
                            col('temp_avg') + (0.5 * col('humidity')/100 * (col('temp_avg') - 14.5)))
                       .otherwise(col('temp_avg'))) \
            .withColumn('is_rainy', when(col('rainfall') > 0, 1).otherwise(0)) \
            .withColumn('rain_category',
                       when(col('rainfall') == 0, 'No Rain')
                       .when(col('rainfall') <= 5, 'Light Rain')
                       .when(col('rainfall') <= 20, 'Moderate Rain') 
                       .when(col('rainfall') <= 50, 'Heavy Rain')
                       .otherwise('Very Heavy Rain')) \
            .withColumn('temp_category',
                       when(col('temp_avg') < 22, 'Cool')
                       .when(col('temp_avg') < 26, 'Comfortable')
                       .when(col('temp_avg') < 30, 'Warm')
                       .when(col('temp_avg') < 35, 'Hot')
                       .otherwise('Very Hot')) \
            .withColumn('humidity_category',
                       when(col('humidity') < 60, 'Low')
                       .when(col('humidity') < 70, 'Moderate')
                       .when(col('humidity') < 80, 'High')
                       .otherwise('Very High'))
        
        print("🔧 Feature engineering completed")
        print("📋 Features created:")
        df_engineered.printSchema()
        
        return df_engineered
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None

def create_weather_prediction_pipeline(df):
    """Membuat pipeline Random Forest untuk prediksi pola cuaca"""
    print("🤖 Creating Random Forest prediction pipeline...")
    
    try:
        # Pilih features untuk prediction
        feature_cols = [
            'temp_min', 'temp_max', 'temp_avg', 'humidity', 
            'sunshine', 'wind_speed', 'month', 'day_of_year',
            'temp_range', 'heat_index', 'is_rainy'
        ]
        
        # Bersihkan data untuk ML (hapus missing values)
        df_ml = df.select(feature_cols + ['rain_category', 'temp_category']).na.drop()
        
        print(f"📊 ML dataset: {df_ml.count()} rows")
        
        # Pipeline untuk prediksi rain_category
        rain_indexer = StringIndexer(inputCol='rain_category', outputCol='rain_label')
        rain_assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
        rain_rf = RandomForestClassifier(
            featuresCol='features',
            labelCol='rain_label',
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        rain_label_converter = IndexToString(
            inputCol='prediction',
            outputCol='predicted_rain_category',
            labels=rain_indexer.fit(df_ml).labels
        )
        
        rain_pipeline = Pipeline(stages=[
            rain_indexer, rain_assembler, rain_rf, rain_label_converter
        ])
        
        # Pipeline untuk prediksi temp_category  
        temp_indexer = StringIndexer(inputCol='temp_category', outputCol='temp_label')
        temp_assembler = VectorAssembler(inputCols=feature_cols, outputCol='temp_features')
        temp_rf = RandomForestClassifier(
            featuresCol='temp_features', 
            labelCol='temp_label',
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        temp_label_converter = IndexToString(
            inputCol='prediction',
            outputCol='predicted_temp_category',
            labels=temp_indexer.fit(df_ml).labels
        )
        
        temp_pipeline = Pipeline(stages=[
            temp_indexer, temp_assembler, temp_rf, temp_label_converter
        ])
        
        return df_ml, rain_pipeline, temp_pipeline, feature_cols
        
    except Exception as e:
        print(f"❌ Error creating pipeline: {e}")
        return None, None, None, None

def train_and_evaluate_models(df_ml, rain_pipeline, temp_pipeline):
    """Train dan evaluasi Random Forest models"""
    print("🎯 Training Random Forest models...")
    
    try:
        # Split data untuk training dan testing
        train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)
        
        print(f"📊 Training data: {train_data.count()} rows")
        print(f"📊 Test data: {test_data.count()} rows")
        
        # Train Rain Prediction Model
        print("🌧️ Training Rain Prediction Model...")
        rain_model = rain_pipeline.fit(train_data)
        rain_predictions = rain_model.transform(test_data)
        
        # Evaluasi Rain Model
        rain_evaluator = MulticlassClassificationEvaluator(
            labelCol='rain_label',
            predictionCol='prediction',
            metricName='accuracy'
        )
        rain_accuracy = rain_evaluator.evaluate(rain_predictions)
        
        # Train Temperature Prediction Model
        print("🌡️ Training Temperature Prediction Model...")
        temp_model = temp_pipeline.fit(train_data)
        temp_predictions = temp_model.transform(test_data)
        
        # Evaluasi Temperature Model
        temp_evaluator = MulticlassClassificationEvaluator(
            labelCol='temp_label',
            predictionCol='prediction', 
            metricName='accuracy'
        )
        temp_accuracy = temp_evaluator.evaluate(temp_predictions)
        
        print(f"✅ Rain Prediction Accuracy: {rain_accuracy:.4f}")
        print(f"✅ Temperature Prediction Accuracy: {temp_accuracy:.4f}")
        
        return rain_model, temp_model, rain_predictions, temp_predictions, train_data, test_data
        
    except Exception as e:
        print(f"❌ Error training models: {e}")
        return None, None, None, None, None, None

def generate_future_predictions(spark, rain_model, temp_model, feature_cols):
    """Generate prediksi untuk periode masa depan"""
    print("🔮 Generating future predictions...")
    
    try:
        # Buat dataset simulasi untuk prediksi masa depan (30 hari ke depan)
        from datetime import datetime, timedelta
        import pandas as pd
        
        # Generate tanggal 30 hari ke depan
        start_date = datetime(2024, 1, 1)
        future_dates = [start_date + timedelta(days=i) for i in range(30)]
        
        # Buat data simulasi berdasarkan pola historis
        future_data = []
        for i, date in enumerate(future_dates):
            # Simulasi berdasarkan pola musiman
            month = date.month
            day_of_year = date.timetuple().tm_yday
            
            # Pola suhu berdasarkan bulan (Indonesia)
            if month in [12, 1, 2]:  # Musim hujan
                temp_avg = 26 + (i % 3)
                humidity = 80 + (i % 10)
                rainfall_prob = 0.7
            elif month in [6, 7, 8]:  # Musim kering
                temp_avg = 28 + (i % 4) 
                humidity = 65 + (i % 8)
                rainfall_prob = 0.2
            else:  # Transisi
                temp_avg = 27 + (i % 3)
                humidity = 75 + (i % 10)
                rainfall_prob = 0.4
            
            future_data.append({
                'temp_min': temp_avg - 3,
                'temp_max': temp_avg + 5,
                'temp_avg': temp_avg,
                'humidity': humidity,
                'sunshine': 6 + (i % 4),
                'wind_speed': 2 + (i % 3),
                'month': month,
                'day_of_year': day_of_year,
                'temp_range': 8,
                'heat_index': temp_avg + (humidity/100 * 2),
                'is_rainy': 1 if (i % 10 < rainfall_prob * 10) else 0
            })
        
        # Convert ke Spark DataFrame
        future_df = spark.createDataFrame(pd.DataFrame(future_data))
        
        # Generate predictions
        rain_future_pred = rain_model.transform(future_df)
        temp_future_pred = temp_model.transform(future_df)
        
        # Gabungkan hasil prediksi
        future_predictions = rain_future_pred.select(
            col('temp_avg'),
            col('humidity'), 
            col('month'),
            col('predicted_rain_category')
        ).join(
            temp_future_pred.select(
                col('temp_avg').alias('temp_avg_2'),
                col('predicted_temp_category')
            ),
            col('temp_avg') == col('temp_avg_2')
        ).drop('temp_avg_2')
        
        print(f"🔮 Generated {future_predictions.count()} future predictions")
        
        return future_predictions
        
    except Exception as e:
        print(f"❌ Error generating future predictions: {e}")
        return None

def save_results_to_gold_layer(spark, rain_predictions, temp_predictions, future_predictions, rain_model, temp_model):
    """Simpan hasil prediksi dan model ke Gold layer"""
    print("💾 Saving results to Gold layer...")
    
    try:
        # 1. Simpan hasil evaluasi model
        model_metrics = spark.createDataFrame([
            ('Rain Prediction Model', 'Random Forest', 'Accuracy', 
             rain_predictions.select('rain_label', 'prediction').rdd.map(
                 lambda row: 1 if row.rain_label == row.prediction else 0
             ).mean()),
            ('Temperature Prediction Model', 'Random Forest', 'Accuracy',
             temp_predictions.select('temp_label', 'prediction').rdd.map(
                 lambda row: 1 if row.temp_label == row.prediction else 0  
             ).mean())
        ], ['model_name', 'algorithm', 'metric', 'value'])
        
        model_metrics.coalesce(1).write.mode('overwrite').parquet(
            'hdfs://namenode:9000/data/gold/ml_model_metrics'
        )
        
        # 2. Simpan feature importance
        rain_rf_model = rain_model.stages[-2]  # Random Forest stage
        temp_rf_model = temp_model.stages[-2]
        
        feature_cols = [
            'temp_min', 'temp_max', 'temp_avg', 'humidity', 
            'sunshine', 'wind_speed', 'month', 'day_of_year',
            'temp_range', 'heat_index', 'is_rainy'
        ]
        
        rain_importance = [(feature_cols[i], float(importance)) 
                          for i, importance in enumerate(rain_rf_model.featureImportances)]
        temp_importance = [(feature_cols[i], float(importance))
                          for i, importance in enumerate(temp_rf_model.featureImportances)]
        
        rain_importance_df = spark.createDataFrame(rain_importance, ['feature', 'importance']) \
            .withColumn('model_type', lit('Rain Prediction'))
        temp_importance_df = spark.createDataFrame(temp_importance, ['feature', 'importance']) \
            .withColumn('model_type', lit('Temperature Prediction'))
        
        feature_importance_df = rain_importance_df.union(temp_importance_df)
        feature_importance_df.coalesce(1).write.mode('overwrite').parquet(
            'hdfs://namenode:9000/data/gold/ml_feature_importance'
        )
        
        # 3. Simpan prediksi masa depan
        if future_predictions:
            future_predictions.coalesce(1).write.mode('overwrite').parquet(
                'hdfs://namenode:9000/data/gold/weather_future_predictions'
            )
        
        # 4. Simpan sample predictions untuk validasi
        sample_predictions = rain_predictions.select(
            'temp_avg', 'humidity', 'rain_category', 'predicted_rain_category',
            when(col('rain_category') == col('predicted_rain_category'), 'Correct')
            .otherwise('Incorrect').alias('prediction_status')
        ).join(
            temp_predictions.select(
                'temp_avg', 'temp_category', 'predicted_temp_category'
            ), 'temp_avg'
        ).limit(100)
        
        sample_predictions.coalesce(1).write.mode('overwrite').parquet(
            'hdfs://namenode:9000/data/gold/ml_sample_predictions'
        )
        
        print("✅ Results saved to Gold layer")
        
        # Tampilkan ringkasan
        print("\n📊 Model Performance Summary:")
        model_metrics.show()
        
        print("\n🎯 Top Feature Importance:")
        feature_importance_df.orderBy(desc('importance')).show(10)
        
        print("\n🔮 Future Predictions Sample:")
        if future_predictions:
            future_predictions.show(10)
        
        print("\n✅ Sample Prediction Results:")
        sample_predictions.show(10)
        
        return True
        
    except Exception as e:
        print(f"❌ Error saving results: {e}")
        return False

def main():
    """Fungsi utama untuk ML prediction pipeline"""
    spark = None
    try:
        # Initialize Spark
        spark = initialize_spark()
        
        # Load dan prepare data
        df = load_and_prepare_data(spark)
        if df is None:
            print("❌ Failed to load data")
            return False
        
        # Create ML pipeline
        df_ml, rain_pipeline, temp_pipeline, feature_cols = create_weather_prediction_pipeline(df)
        if df_ml is None:
            print("❌ Failed to create ML pipeline")
            return False
        
        # Train dan evaluasi models
        rain_model, temp_model, rain_pred, temp_pred, train_data, test_data = train_and_evaluate_models(
            df_ml, rain_pipeline, temp_pipeline
        )
        if rain_model is None:
            print("❌ Failed to train models")
            return False
        
        # Generate future predictions
        future_pred = generate_future_predictions(spark, rain_model, temp_model, feature_cols)
        
        # Save results ke Gold layer
        success = save_results_to_gold_layer(
            spark, rain_pred, temp_pred, future_pred, rain_model, temp_model
        )
        
        if success:
            print("\n🎉 Weather Prediction ML Pipeline completed successfully!")
            print("📊 Results saved to Gold layer:")
            print("   - Model metrics: /data/gold/ml_model_metrics")
            print("   - Feature importance: /data/gold/ml_feature_importance") 
            print("   - Future predictions: /data/gold/weather_future_predictions")
            print("   - Sample predictions: /data/gold/ml_sample_predictions")
        
        return success
        
    except Exception as e:
        print(f"❌ Error in main pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
