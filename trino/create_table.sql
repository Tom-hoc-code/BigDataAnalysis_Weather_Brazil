-- =========================================================
-- CREATE SCHEMAS
-- =========================================================
CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3://s3-group2-bigdata/silver/');

CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3://s3-group2-bigdata/gold/');


-- =========================================================
-- SILVER LAYER
-- =========================================================

-- 1) DIM LOCATION
CREATE TABLE IF NOT EXISTS iceberg.silver.dim_location (
    location_key VARCHAR,
    station_code VARCHAR,
    region VARCHAR,
    state VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD',
    sorted_by = ARRAY['location_key']
);

-- 2) DIM DATE TIME
-- Grain: 1 row = 1 date + 1 hour
CREATE TABLE IF NOT EXISTS iceberg.silver.dim_date_time (
    date_time_key VARCHAR,
    date DATE,
    hour VARCHAR,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    season VARCHAR
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD',
    sorted_by = ARRAY['date', 'hour']
);

-- 3) DIM WEATHER CONDITION
-- Theo schema FINAL của bạn: temp_category, humidity_category, wind_level
CREATE TABLE IF NOT EXISTS iceberg.silver.dim_weather_condition (
    condition_key VARCHAR,
    temp_category VARCHAR,
    humidity_category VARCHAR,
    wind_level VARCHAR
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD',
    sorted_by = ARRAY['condition_key']
);

-- 4) DIM ALERT
CREATE TABLE IF NOT EXISTS iceberg.silver.dim_alert (
    alert_key VARCHAR,
    alert_type VARCHAR,
    severity VARCHAR
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD',
    sorted_by = ARRAY['alert_key']
);

-- 5) FACT HOURLY OBSERVATION
-- Grain: 1 station + 1 date + 1 hour
CREATE TABLE IF NOT EXISTS iceberg.silver.fact_hourly_observation (
    fact_key VARCHAR,
    observation_id VARCHAR,
    date_time_key VARCHAR,
    location_key VARCHAR,
    condition_key VARCHAR,
    alert_key VARCHAR,
    source_file VARCHAR,

    rainfall_hourly DOUBLE,
    pressure DOUBLE,
    pressure_max DOUBLE,
    pressure_min DOUBLE,
    solar_radiation DOUBLE,

    temperature DOUBLE,
    temperature_max DOUBLE,
    temperature_min DOUBLE,

    dew_point DOUBLE,
    dew_point_max DOUBLE,
    dew_point_min DOUBLE,

    humidity DOUBLE,
    humidity_max DOUBLE,
    humidity_min DOUBLE,

    wind_direction DOUBLE,
    wind_speed DOUBLE,
    wind_gust DOUBLE
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD',
    sorted_by = ARRAY['date_time_key', 'location_key']
);


-- =========================================================
-- GOLD LAYER
-- =========================================================

-- 6) FACT WEATHER AGGREGATE
CREATE TABLE IF NOT EXISTS iceberg.gold.fact_weather_aggregate (
    date_time_key VARCHAR,
    region VARCHAR,
    state VARCHAR,
    avg_temp DOUBLE,
    min_temp DOUBLE,
    max_temp DOUBLE,
    total_rainfall DOUBLE,
    wind_speed DOUBLE,
    max_gust DOUBLE,
    dominant_direction VARCHAR
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD',
    sorted_by = ARRAY['date_time_key']
);

-- 7) FACT PRECIPITATION ANALYSIS
CREATE TABLE IF NOT EXISTS iceberg.gold.fact_precipitation_analysis (
    date_time_key VARCHAR,
    region VARCHAR,
    state VARCHAR,
    total_rainfall DOUBLE,
    drought_index DOUBLE,
    flood_risk DOUBLE
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD',
    sorted_by = ARRAY['date_time_key']
);

-- 8) FACT EXTREME EVENTS
CREATE TABLE IF NOT EXISTS iceberg.gold.fact_extreme_events (
    date_time_key VARCHAR,
    region VARCHAR,
    state VARCHAR,
    event_type VARCHAR,
    duration INTEGER,
    severity VARCHAR
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD',
    sorted_by = ARRAY['date_time_key', 'event_type']
);

-- 9) FACT MONTHLY CLIMATE SNAPSHOT
CREATE TABLE IF NOT EXISTS iceberg.gold.fact_monthly_climate_snapshot (
    region VARCHAR,
    state VARCHAR,
    month INTEGER,
    year INTEGER,
    avg_temp_monthly DOUBLE,
    total_rainfall_monthly DOUBLE,
    rainy_days_count BIGINT,
    temp_anomaly DOUBLE
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD'
);

-- 10) FACT REGIONAL RISK DAILY SNAPSHOT
CREATE TABLE IF NOT EXISTS iceberg.gold.fact_regional_risk_daily_snapshot (
    day INTEGER,
    month INTEGER,
    year INTEGER,
    region VARCHAR,
    state VARCHAR,
    alert_type VARCHAR,
    cumulative_alert_duration INTEGER,
    risk_index_score DOUBLE
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD'
);

-- 11) FACT EXTREME EVENT YEARLY SNAPSHOT
CREATE TABLE IF NOT EXISTS iceberg.gold.fact_extreme_event_yearly_snapshot (
    year INTEGER,
    region VARCHAR,
    state VARCHAR,
    extreme_event_count BIGINT,
    total_event_duration BIGINT,
    avg_severity DOUBLE
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD'
);

-- 12) FACT WEATHER WEATHER MONTHLY
-- Giữ theo đúng tên trong schema bạn gửi
CREATE TABLE IF NOT EXISTS iceberg.gold.fact_weather_monthly (
    date_time_key VARCHAR,
    month INTEGER,
    year INTEGER,
    region VARCHAR,
    state VARCHAR,
    avg_rainfall DOUBLE,
    drought_level DOUBLE
)
WITH (
    format = 'PARQUET',
    compression_codec = 'ZSTD',
    sorted_by = ARRAY['date_time_key']
);
