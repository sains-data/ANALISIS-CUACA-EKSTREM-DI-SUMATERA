-- Create database if not exists
CREATE DATABASE IF NOT EXISTS weather_db;
USE weather_db;

-- Create table for weather data
CREATE TABLE IF NOT EXISTS weather_data (
    date DATE,
    temp_min DOUBLE,
    temp_max DOUBLE,
    temp_avg DOUBLE,
    humidity DOUBLE,
    rainfall DOUBLE,
    sunshine_duration DOUBLE,
    wind_speed_max DOUBLE,
    wind_direction_max DOUBLE,
    wind_speed_avg DOUBLE,
    wind_direction STRING
)
PARTITIONED BY (processing_date DATE)
STORED AS PARQUET
LOCATION '/data/gold/weather_data';

-- Refresh table metadata
MSCK REPAIR TABLE weather_data;

-- Create view for complete data
CREATE OR REPLACE VIEW complete_weather_data AS
SELECT 
    date,
    temp_min,
    temp_max,
    temp_avg,
    humidity,
    rainfall,
    sunshine_duration,
    wind_speed_max,
    wind_direction_max,
    wind_speed_avg,
    wind_direction,
    processing_date
FROM weather_data;
