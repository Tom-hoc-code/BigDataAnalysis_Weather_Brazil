from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year

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
SILVER_PATH = "s3a://s3-group2-bigdata/silver/"
GOLD_PATH = "s3a://s3-group2-bigdata/gold/"
CATALOG = "weather_catalog.weather_gold"


# =====================================
# SPARK + ICEBERG
# =====================================
spark = (
    SparkSession.builder
    .appName("silver_to_gold_weather")
    .config(
        "spark.sql.catalog.weather_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.weather_catalog.type", "hadoop")
    .config(
        "spark.sql.catalog.weather_catalog.warehouse",
        GOLD_PATH
    )
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS weather_catalog.weather_gold")


# =====================================
# HELPER SAVE ICEBERG
# =====================================
def save_iceberg(df, table_name):
    (
        df.writeTo(f"{CATALOG}.{table_name}")
        .tableProperty("location", GOLD_PATH + table_name + "/")
        .createOrReplace()
    )


# =====================================
# READ ALL SILVER TABLES
# =====================================
dim_location = spark.read.parquet(SILVER_PATH + "dim_location/")
dim_date = spark.read.parquet(SILVER_PATH + "dim_date/")
dim_time = spark.read.parquet(SILVER_PATH + "dim_time/")
dim_weather_condition = spark.read.parquet(
    SILVER_PATH + "dim_weather_condition/"
)
dim_alert = spark.read.parquet(SILVER_PATH + "dim_alert/")
fact_hourly = spark.read.parquet(
    SILVER_PATH + "fact_hourly_weather/"
)


# =====================================
# BUILD BUSINESS DATAFRAME
# =====================================
df = (
    fact_hourly
    .join(dim_date, "date_key", "left")
    .join(dim_time, "time_key", "left")
    .join(dim_location, "location_key", "left")
    .join(dim_weather_condition, "condition_key", "left")
    .join(dim_alert, "alert_key", "left")
)

if "date" in df.columns:
    df = df.withColumn("month", month("date"))
    df = df.withColumn("year", year("date"))


# =====================================
# BUILD 7 BUSINESS FACTS
# =====================================
fact_weather_aggregate = build_fact_weather_aggregate(df)
fact_precip = build_fact_precipitation_analysis(df)
fact_extreme = build_fact_extreme_events(df)
fact_monthly = build_fact_monthly_climate_snapshot(df)
fact_weather_monthly = build_fact_weather_monthly(fact_precip)
fact_risk = build_fact_regional_risk_daily_snapshot(df)
fact_yearly = build_fact_extreme_event_yearly_snapshot(fact_extreme)


# =====================================
# SAVE BASE STAR SCHEMA
# =====================================
# save_iceberg(dim_location, "dim_location")
# save_iceberg(dim_date, "dim_date")
# save_iceberg(dim_time, "dim_time")
# save_iceberg(dim_weather_condition, "dim_weather_condition")
# save_iceberg(dim_alert, "dim_alert")
# save_iceberg(fact_hourly, "fact_hourly_observation")


# =====================================
# SAVE 7 BUSINESS FACTS
# =====================================
save_iceberg(fact_weather_aggregate, "fact_weather_aggregate")
save_iceberg(fact_precip, "fact_precipitation_analysis")
save_iceberg(fact_extreme, "fact_extreme_events")
save_iceberg(fact_monthly, "fact_monthly_climate_snapshot")
save_iceberg(fact_weather_monthly, "fact_weather_monthly")
save_iceberg(fact_risk, "fact_regional_risk_daily_snapshot")
save_iceberg(fact_yearly, "fact_extreme_event_yearly_snapshot")

print("✅ SILVER TO GOLD ICEBERG COMPLETED")