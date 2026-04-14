from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, month, year, dayofmonth
import random

from fact_weather_aggregate import build_fact_weather_aggregate
from fact_precipitation_analysis import build_fact_precipitation_analysis
from fact_extreme_events import build_fact_extreme_events
from fact_monthly_climate_snapshot import build_fact_monthly_climate_snapshot
from fact_weather_monthly import build_fact_weather_monthly
from fact_regional_risk_daily_snapshot import build_fact_regional_risk_daily_snapshot
from fact_extreme_event_yearly_snapshot import (
    build_fact_extreme_event_yearly_snapshot
)

# =====================================
# PATH + CATALOG
# =====================================
WAREHOUSE_PATH = "s3a://s3-group2-bigdata/"
CATALOG = "weather_catalog"

SILVER_DB = "silver"
GOLD_DB = "gold"

# =====================================
# SPARK + ICEBERG + GLUE
# Làm tương tự bronze_to_silver
# =====================================
spark = (
    SparkSession.builder
    .appName("silver_to_gold_weather_dw_iceberg")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.memoryOverhead", "1g")
    .config("spark.sql.shuffle.partitions", "120")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        f"spark.sql.catalog.{CATALOG}",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.warehouse",
        WAREHOUSE_PATH
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.glue.region",
        "ap-southeast-2"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Tạo namespace gold nếu chưa có
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{GOLD_DB}")



# =====================================
# READ ALL SILVER TABLES FROM ICEBERG
# Không đọc parquet trực tiếp nữa
# =====================================
print("READ SILVER ICEBERG TABLES")

dim_location = spark.table(f"{CATALOG}.{SILVER_DB}.dim_location")
dim_date_time = spark.table(f"{CATALOG}.{SILVER_DB}.dim_date_time")
dim_weather_condition = spark.table(f"{CATALOG}.{SILVER_DB}.dim_weather_condition")
dim_alert = spark.table(f"{CATALOG}.{SILVER_DB}.dim_alert")
fact_hourly = spark.table(f"{CATALOG}.{SILVER_DB}.fact_hourly_observation")


# # =====================================
# # BUILD BUSINESS DATAFRAME
# # Join đúng theo khóa của silver hiện tại
# # =====================================
print("BUILD BUSINESS DATAFRAME")

df = (
    fact_hourly
    .join(dim_date_time, "date_time_key", "left")
    .join(dim_location, "location_key", "left")
    .join(dim_weather_condition, "condition_key", "left")
    .join(dim_alert, "alert_key", "left")
)

# Nếu chưa có month/year thì bổ sung từ date
if "date" in df.columns:
    if "day" not in df.columns:
        df = df.withColumn("day", dayofmonth(col("date")))
    if "month" not in df.columns:
        df = df.withColumn("month", month(col("date")))
    if "year" not in df.columns:
        df = df.withColumn("year", year(col("date")))


# =====================================
# BUILD 7 BUSINESS FACTS
# =====================================
print("BUILD GOLD FACT TABLES")
# def get_temp_category(temperature):
#     if temperature < 10:
#         return "cold"
#     elif temperature < 25:
#         return "normal"
#     return "hot"

# def get_humidity_category(humidity):
#     if humidity < 40:
#         return "dry"
#     elif humidity < 70:
#         return "normal"
#     return "humid"

# def get_wind_level(wind_speed):
#     if wind_speed < 3:
#         return "low"
#     elif wind_speed < 8:
#         return "medium"
#     return "high"

# def get_alert_type(temperature, humidity, wind_speed):
#     if temperature > 40:
#         return "heatwave"
#     elif humidity > 90:
#         return "storm_risk"
#     elif wind_speed > 15:
#         return "windstorm"
#     return "normal"

# def get_severity(alert_type):
#     if alert_type == "normal":
#         return "low"
#     elif alert_type == "windstorm":
#         return "medium"
#     return "high"

# rows = []

# for i in range(10):
#     temperature = round(random.uniform(5, 45), 1)
#     temperature_max = round(temperature + random.uniform(0.2, 2.5), 1)
#     temperature_min = round(temperature - random.uniform(0.2, 2.5), 1)

#     humidity = round(random.uniform(30, 95), 1)
#     humidity_max = min(100.0, round(humidity + random.uniform(1, 8), 1))
#     humidity_min = max(0.0, round(humidity - random.uniform(1, 8), 1))

#     wind_speed = round(random.uniform(0.5, 20), 1)
#     wind_gust = round(wind_speed + random.uniform(0.5, 5), 1)

#     dew_point = round(random.uniform(5, 20), 1)
#     dew_point_max = round(dew_point + random.uniform(0.2, 2.0), 1)
#     dew_point_min = round(dew_point - random.uniform(0.2, 2.0), 1)

#     pressure = round(random.uniform(900, 920), 1)
#     pressure_max = round(pressure + random.uniform(0.1, 1.5), 1)
#     pressure_min = round(pressure - random.uniform(0.1, 1.5), 1)

#     rainfall_hourly = round(random.uniform(0, 5), 1)
#     solar_radiation = round(random.uniform(2000, 3200), 1)
#     wind_direction = round(random.uniform(100, 200), 1)

#     hour = random.randint(0, 23)
#     day = 20 + (i // 5)
#     month = 8
#     year = 2019
#     date = f"2019-08-{day:02d}"
#     season = "Summer"

#     temp_category = get_temp_category(temperature)
#     humidity_category = get_humidity_category(humidity)
#     wind_level = get_wind_level(wind_speed)
#     alert_type = get_alert_type(temperature, humidity, wind_speed)
#     severity = get_severity(alert_type)

#     rows.append((
#         f"fact_{i:03d}",
#         205169 + i,
#         f"dt_{i:03d}",
#         "loc_001",
#         f"cond_{(i % 3) + 1:03d}",
#         f"alert_{(i % 2) + 1:03d}",
#         "s3a://s3-group2-bigdata/datasource/south.csv",
#         rainfall_hourly,

#         pressure,
#         pressure_max,
#         pressure_min,
#         solar_radiation,

#         temperature,
#         temperature_max,
#         temperature_min,

#         dew_point,
#         dew_point_max,
#         dew_point_min,

#         humidity,
#         humidity_max,
#         humidity_min,

#         wind_direction,
#         wind_speed,
#         wind_gust,

#         "A816",
#         "s",
#         "sc",
#         -26.406499,
#         -52.850366,

#         date,
#         hour,
#         day,
#         month,
#         year,
#         season,

#         temp_category,
#         humidity_category,
#         wind_level,
#         alert_type,
#         severity
#     ))

# columns = [
#     "fact_key",
#     "observation_id",
#     "date_time_key",
#     "location_key",
#     "condition_key",
#     "alert_key",
#     "source_file",
#     "rainfall_hourly",

#     "pressure",
#     "pressure_max",
#     "pressure_min",
#     "solar_radiation",

#     "temperature",
#     "temperature_max",
#     "temperature_min",

#     "dew_point",
#     "dew_point_max",
#     "dew_point_min",

#     "humidity",
#     "humidity_max",
#     "humidity_min",

#     "wind_direction",
#     "wind_speed",
#     "wind_gust",

#     "station_code",
#     "region",
#     "state",
#     "latitude",
#     "longitude",

#     "date",
#     "hour",
#     "day",
#     "month",
#     "year",
#     "season",

#     "temp_category",
#     "humidity_category",
#     "wind_level",
#     "alert_type",
#     "severity"
# ]

# df = spark.createDataFrame(rows, columns)
fact_weather_aggregate = build_fact_weather_aggregate(df) #đã xong
fact_precip = build_fact_precipitation_analysis(df) # đã xong
fact_extreme = build_fact_extreme_events(df) # Đã xong
fact_monthly = build_fact_monthly_climate_snapshot(df) # đã xong
fact_weather_monthly = build_fact_weather_monthly(fact_precip, dim_date_time) # đã xong

fact_risk = build_fact_regional_risk_daily_snapshot(df) # đã xong
fact_yearly = build_fact_extreme_event_yearly_snapshot(dim_date_time, fact_extreme) # đã xong



# =====================================
# OPTIONAL DEBUG
# =====================================
print("DEBUG SAMPLE")
# fact_weather_aggregate.show(3, False)
# fact_precip.show(3, False)
# fact_extreme.show(3, False)
# fact_monthly.show(3, False)
# fact_weather_monthly.show(3, False)
# fact_risk.show(3, False)
# fact_yearly.show(3, False)


# =====================================
# WRITE GOLD ICEBERG TABLES
# Ghi giống bronze_to_silver
# =====================================
print("WRITE GOLD ICEBERG TABLES")
fact_weather_aggregate.writeTo(f"{CATALOG}.{GOLD_DB}.fact_weather_aggregate").overwrite(lit(True))
fact_precip.writeTo(f"{CATALOG}.{GOLD_DB}.fact_precipitation_analysis").overwrite(lit(True))
fact_extreme.writeTo(f"{CATALOG}.{GOLD_DB}.fact_extreme_events").overwrite(lit(True))
fact_monthly.writeTo(f"{CATALOG}.{GOLD_DB}.fact_monthly_climate_snapshot").overwrite(lit(True))
fact_weather_monthly.writeTo(f"{CATALOG}.{GOLD_DB}.fact_weather_monthly").overwrite(lit(True))
fact_risk.writeTo(f"{CATALOG}.{GOLD_DB}.fact_regional_risk_daily_snapshot").overwrite(lit(True))
fact_yearly.writeTo(f"{CATALOG}.{GOLD_DB}.fact_extreme_event_yearly_snapshot").overwrite(lit(True)) 

print("SILVER TO GOLD ICEBERG LOAD COMPLETED")