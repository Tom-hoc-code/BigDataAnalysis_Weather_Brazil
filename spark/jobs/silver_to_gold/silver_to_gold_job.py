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