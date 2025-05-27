-- Weather Data Analytics Script untuk Hive dengan PostgreSQL
-- Membaca data dari Silver layer HDFS dan melakukan analytics
-- Hasil analytics disimpan ke Gold layer

-- 1. CREATE EXTERNAL TABLE untuk membaca data dari Silver layer
DROP TABLE IF EXISTS weather_silver;
CREATE EXTERNAL TABLE weather_silver (
    TANGGAL STRING,
    TN DOUBLE,
    TX DOUBLE,
    TAVG DOUBLE,
    RH_AVG DOUBLE,
    RR DOUBLE,
    SS DOUBLE,
    FF_X DOUBLE,
    DDD_X STRING,
    FF_AVG DOUBLE,
    DDD_CAR STRING,
    source_file STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/silver/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2. Buat tabel untuk menyimpan hasil analytics di Gold layer
DROP TABLE IF EXISTS weather_analytics_monthly;
CREATE TABLE weather_analytics_monthly (
    tahun INT,
    bulan INT,
    bulan_nama STRING,
    avg_temp_min DOUBLE,
    avg_temp_max DOUBLE,
    avg_temp_avg DOUBLE,
    avg_humidity DOUBLE,
    total_rainfall DOUBLE,
    avg_sunshine DOUBLE,
    avg_wind_speed DOUBLE,
    dominant_wind_direction STRING,
    extreme_temp_min DOUBLE,
    extreme_temp_max DOUBLE,
    rainy_days INT,
    hot_days INT,
    cold_days INT,
    total_records INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/gold/monthly_analytics/';

-- 3. Analytics bulanan
INSERT OVERWRITE TABLE weather_analytics_monthly
SELECT 
    YEAR(TO_DATE(TANGGAL, 'dd-MM-yyyy')) as tahun,
    MONTH(TO_DATE(TANGGAL, 'dd-MM-yyyy')) as bulan,
    CASE MONTH(TO_DATE(TANGGAL, 'dd-MM-yyyy'))
        WHEN 1 THEN 'Januari'
        WHEN 2 THEN 'Februari'  
        WHEN 3 THEN 'Maret'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'Mei'
        WHEN 6 THEN 'Juni'
        WHEN 7 THEN 'Juli'
        WHEN 8 THEN 'Agustus'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Oktober'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'Desember'
    END as bulan_nama,
    ROUND(AVG(TN), 2) as avg_temp_min,
    ROUND(AVG(TX), 2) as avg_temp_max,
    ROUND(AVG(TAVG), 2) as avg_temp_avg,
    ROUND(AVG(RH_AVG), 2) as avg_humidity,
    ROUND(SUM(RR), 2) as total_rainfall,
    ROUND(AVG(SS), 2) as avg_sunshine,
    ROUND(AVG(FF_AVG), 2) as avg_wind_speed,
    -- Mencari arah angin yang paling dominan
    FIRST_VALUE(DDD_CAR) OVER (
        PARTITION BY YEAR(TO_DATE(TANGGAL, 'dd-MM-yyyy')), MONTH(TO_DATE(TANGGAL, 'dd-MM-yyyy'))
        ORDER BY COUNT(DDD_CAR) DESC
    ) as dominant_wind_direction,
    ROUND(MIN(TN), 2) as extreme_temp_min,
    ROUND(MAX(TX), 2) as extreme_temp_max,
    SUM(CASE WHEN RR > 0 THEN 1 ELSE 0 END) as rainy_days,
    SUM(CASE WHEN TX > 32 THEN 1 ELSE 0 END) as hot_days,
    SUM(CASE WHEN TN < 20 THEN 1 ELSE 0 END) as cold_days,
    COUNT(*) as total_records
FROM weather_silver
WHERE TANGGAL IS NOT NULL 
    AND TN IS NOT NULL 
    AND TX IS NOT NULL
GROUP BY 
    YEAR(TO_DATE(TANGGAL, 'dd-MM-yyyy')),
    MONTH(TO_DATE(TANGGAL, 'dd-MM-yyyy'))
ORDER BY tahun, bulan;

-- 4. Buat tabel untuk analytics tahunan
DROP TABLE IF EXISTS weather_analytics_yearly;
CREATE TABLE weather_analytics_yearly (
    tahun INT,
    avg_temp_min DOUBLE,
    avg_temp_max DOUBLE,
    avg_temp_avg DOUBLE,
    avg_humidity DOUBLE,
    total_rainfall DOUBLE,
    avg_sunshine DOUBLE,
    avg_wind_speed DOUBLE,
    extreme_temp_min DOUBLE,
    extreme_temp_max DOUBLE,
    total_rainy_days INT,
    total_hot_days INT,
    total_cold_days INT,
    wettest_month STRING,
    driest_month STRING,
    hottest_month STRING,
    coldest_month STRING,
    total_records INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/gold/yearly_analytics/';

-- 5. Analytics tahunan
INSERT OVERWRITE TABLE weather_analytics_yearly
SELECT 
    tahun,
    ROUND(AVG(avg_temp_min), 2) as avg_temp_min,
    ROUND(AVG(avg_temp_max), 2) as avg_temp_max,
    ROUND(AVG(avg_temp_avg), 2) as avg_temp_avg,
    ROUND(AVG(avg_humidity), 2) as avg_humidity,
    ROUND(SUM(total_rainfall), 2) as total_rainfall,
    ROUND(AVG(avg_sunshine), 2) as avg_sunshine,
    ROUND(AVG(avg_wind_speed), 2) as avg_wind_speed,
    MIN(extreme_temp_min) as extreme_temp_min,
    MAX(extreme_temp_max) as extreme_temp_max,
    SUM(rainy_days) as total_rainy_days,
    SUM(hot_days) as total_hot_days,
    SUM(cold_days) as total_cold_days,
    -- Bulan terbasah
    FIRST_VALUE(bulan_nama) OVER (
        PARTITION BY tahun ORDER BY total_rainfall DESC
    ) as wettest_month,
    -- Bulan terkering  
    FIRST_VALUE(bulan_nama) OVER (
        PARTITION BY tahun ORDER BY total_rainfall ASC
    ) as driest_month,
    -- Bulan terpanas
    FIRST_VALUE(bulan_nama) OVER (
        PARTITION BY tahun ORDER BY avg_temp_max DESC
    ) as hottest_month,
    -- Bulan terdingin
    FIRST_VALUE(bulan_nama) OVER (
        PARTITION BY tahun ORDER BY avg_temp_min ASC
    ) as coldest_month,
    SUM(total_records) as total_records
FROM weather_analytics_monthly
GROUP BY tahun
ORDER BY tahun;

-- 6. Buat tabel untuk weather patterns dan trends
DROP TABLE IF EXISTS weather_patterns;
CREATE TABLE weather_patterns (
    pattern_type STRING,
    category STRING,
    value_range STRING,
    frequency INT,
    percentage DOUBLE,
    avg_associated_temp DOUBLE,
    avg_associated_humidity DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/gold/weather_patterns/';

-- 7. Analisis pola cuaca
INSERT OVERWRITE TABLE weather_patterns
SELECT * FROM (
    -- Pola curah hujan
    SELECT 
        'Rainfall Pattern' as pattern_type,
        CASE 
            WHEN RR = 0 THEN 'No Rain'
            WHEN RR > 0 AND RR <= 5 THEN 'Light Rain'
            WHEN RR > 5 AND RR <= 20 THEN 'Moderate Rain'
            WHEN RR > 20 AND RR <= 50 THEN 'Heavy Rain'
            WHEN RR > 50 THEN 'Very Heavy Rain'
        END as category,
        CASE 
            WHEN RR = 0 THEN '0 mm'
            WHEN RR > 0 AND RR <= 5 THEN '0-5 mm'
            WHEN RR > 5 AND RR <= 20 THEN '5-20 mm'
            WHEN RR > 20 AND RR <= 50 THEN '20-50 mm'
            WHEN RR > 50 THEN '>50 mm'
        END as value_range,
        COUNT(*) as frequency,
        ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM weather_silver WHERE RR IS NOT NULL)), 2) as percentage,
        ROUND(AVG(TAVG), 2) as avg_associated_temp,
        ROUND(AVG(RH_AVG), 2) as avg_associated_humidity
    FROM weather_silver
    WHERE RR IS NOT NULL
    GROUP BY 
        CASE 
            WHEN RR = 0 THEN 'No Rain'
            WHEN RR > 0 AND RR <= 5 THEN 'Light Rain'
            WHEN RR > 5 AND RR <= 20 THEN 'Moderate Rain'
            WHEN RR > 20 AND RR <= 50 THEN 'Heavy Rain'
            WHEN RR > 50 THEN 'Very Heavy Rain'
        END,
        CASE 
            WHEN RR = 0 THEN '0 mm'
            WHEN RR > 0 AND RR <= 5 THEN '0-5 mm'
            WHEN RR > 5 AND RR <= 20 THEN '5-20 mm'
            WHEN RR > 20 AND RR <= 50 THEN '20-50 mm'
            WHEN RR > 50 THEN '>50 mm'
        END
        
    UNION ALL
    
    -- Pola suhu
    SELECT 
        'Temperature Pattern' as pattern_type,
        CASE 
            WHEN TAVG < 22 THEN 'Cool'
            WHEN TAVG >= 22 AND TAVG < 26 THEN 'Comfortable'
            WHEN TAVG >= 26 AND TAVG < 30 THEN 'Warm'
            WHEN TAVG >= 30 AND TAVG < 35 THEN 'Hot'
            WHEN TAVG >= 35 THEN 'Very Hot'
        END as category,
        CASE 
            WHEN TAVG < 22 THEN '<22°C'
            WHEN TAVG >= 22 AND TAVG < 26 THEN '22-26°C'
            WHEN TAVG >= 26 AND TAVG < 30 THEN '26-30°C'
            WHEN TAVG >= 30 AND TAVG < 35 THEN '30-35°C'
            WHEN TAVG >= 35 THEN '>35°C'
        END as value_range,
        COUNT(*) as frequency,
        ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM weather_silver WHERE TAVG IS NOT NULL)), 2) as percentage,
        ROUND(AVG(TAVG), 2) as avg_associated_temp,
        ROUND(AVG(RH_AVG), 2) as avg_associated_humidity
    FROM weather_silver
    WHERE TAVG IS NOT NULL
    GROUP BY 
        CASE 
            WHEN TAVG < 22 THEN 'Cool'
            WHEN TAVG >= 22 AND TAVG < 26 THEN 'Comfortable'
            WHEN TAVG >= 26 AND TAVG < 30 THEN 'Warm'
            WHEN TAVG >= 30 AND TAVG < 35 THEN 'Hot'
            WHEN TAVG >= 35 THEN 'Very Hot'
        END,
        CASE 
            WHEN TAVG < 22 THEN '<22°C'
            WHEN TAVG >= 22 AND TAVG < 26 THEN '22-26°C'
            WHEN TAVG >= 26 AND TAVG < 30 THEN '26-30°C'
            WHEN TAVG >= 30 AND TAVG < 35 THEN '30-35°C'
            WHEN TAVG >= 35 THEN '>35°C'
        END
        
    UNION ALL
    
    -- Pola kelembaban
    SELECT 
        'Humidity Pattern' as pattern_type,
        CASE 
            WHEN RH_AVG < 60 THEN 'Low Humidity'
            WHEN RH_AVG >= 60 AND RH_AVG < 70 THEN 'Moderate Humidity'
            WHEN RH_AVG >= 70 AND RH_AVG < 80 THEN 'High Humidity'
            WHEN RH_AVG >= 80 THEN 'Very High Humidity'
        END as category,
        CASE 
            WHEN RH_AVG < 60 THEN '<60%'
            WHEN RH_AVG >= 60 AND RH_AVG < 70 THEN '60-70%'
            WHEN RH_AVG >= 70 AND RH_AVG < 80 THEN '70-80%'
            WHEN RH_AVG >= 80 THEN '>80%'
        END as value_range,
        COUNT(*) as frequency,
        ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM weather_silver WHERE RH_AVG IS NOT NULL)), 2) as percentage,
        ROUND(AVG(TAVG), 2) as avg_associated_temp,
        ROUND(AVG(RH_AVG), 2) as avg_associated_humidity
    FROM weather_silver
    WHERE RH_AVG IS NOT NULL
    GROUP BY 
        CASE 
            WHEN RH_AVG < 60 THEN 'Low Humidity'
            WHEN RH_AVG >= 60 AND RH_AVG < 70 THEN 'Moderate Humidity'
            WHEN RH_AVG >= 70 AND RH_AVG < 80 THEN 'High Humidity'
            WHEN RH_AVG >= 80 THEN 'Very High Humidity'
        END,
        CASE 
            WHEN RH_AVG < 60 THEN '<60%'
            WHEN RH_AVG >= 60 AND RH_AVG < 70 THEN '60-70%'
            WHEN RH_AVG >= 70 AND RH_AVG < 80 THEN '70-80%'
            WHEN RH_AVG >= 80 THEN '>80%'
        END
) combined_patterns
ORDER BY pattern_type, frequency DESC;

-- 8. Tabel summary untuk dashboard
DROP TABLE IF EXISTS weather_dashboard_summary;
CREATE TABLE weather_dashboard_summary (
    metric_name STRING,
    metric_value DOUBLE,
    metric_unit STRING,
    metric_category STRING,
    last_updated STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/gold/dashboard_summary/';

-- 9. Insert summary metrics untuk dashboard
INSERT OVERWRITE TABLE weather_dashboard_summary
SELECT * FROM (
    SELECT 
        'Total Records' as metric_name,
        CAST(COUNT(*) as DOUBLE) as metric_value,
        'count' as metric_unit,
        'General' as metric_category,
        CURRENT_TIMESTAMP() as last_updated
    FROM weather_silver
    
    UNION ALL
    
    SELECT 
        'Average Temperature' as metric_name,
        ROUND(AVG(TAVG), 2) as metric_value,
        '°C' as metric_unit,
        'Temperature' as metric_category,
        CURRENT_TIMESTAMP() as last_updated
    FROM weather_silver WHERE TAVG IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'Average Humidity' as metric_name,
        ROUND(AVG(RH_AVG), 2) as metric_value,
        '%' as metric_unit,
        'Humidity' as metric_category,
        CURRENT_TIMESTAMP() as last_updated
    FROM weather_silver WHERE RH_AVG IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'Total Rainfall' as metric_name,
        ROUND(SUM(RR), 2) as metric_value,
        'mm' as metric_unit,
        'Precipitation' as metric_category,
        CURRENT_TIMESTAMP() as last_updated
    FROM weather_silver WHERE RR IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'Average Wind Speed' as metric_name,
        ROUND(AVG(FF_AVG), 2) as metric_value,
        'm/s' as metric_unit,
        'Wind' as metric_category,
        CURRENT_TIMESTAMP() as last_updated
    FROM weather_silver WHERE FF_AVG IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'Data Coverage Days' as metric_name,
        CAST(COUNT(DISTINCT TANGGAL) as DOUBLE) as metric_value,
        'days' as metric_unit,
        'General' as metric_category,
        CURRENT_TIMESTAMP() as last_updated
    FROM weather_silver WHERE TANGGAL IS NOT NULL
) summary_metrics;

-- 10. Tampilkan hasil analytics
SELECT 'MONTHLY ANALYTICS PREVIEW' as info;
SELECT * FROM weather_analytics_monthly LIMIT 10;

SELECT 'YEARLY ANALYTICS PREVIEW' as info;
SELECT * FROM weather_analytics_yearly;

SELECT 'WEATHER PATTERNS PREVIEW' as info;
SELECT * FROM weather_patterns LIMIT 15;

SELECT 'DASHBOARD SUMMARY' as info;
SELECT * FROM weather_dashboard_summary;
