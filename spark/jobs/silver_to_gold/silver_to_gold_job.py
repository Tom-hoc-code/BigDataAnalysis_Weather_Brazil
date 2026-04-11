from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year

from fact_weather_aggregate import build_fact_weather_aggregate
from fact_precipitation_analysis import build_fact_precipitation_analysis
from fact_extreme_events import build_fact_extreme_events
from fact_monthly_climate_snapshot import build_fact_monthly_climate_snapshot
from fact_weather_monthly import build_fact_weather_monthly
from fact_regional_risk_daily_snapshot import build_fact_regional_risk_daily_snapshot
from fact_extreme_event_yearly_snapshot import build_fact_extreme_event_yearly_snapshot


# =====================================
# PATH
# =====================================
SILVER_PATH = "s3a://s3-group2-bigdata/silver/"
GOLD_PATH = "s3a://s3-group2-bigdata/gold/"


# =====================================
# SPARK
# =====================================
spark = (
    SparkSession.builder
    .appName("silver_to_gold_weather")
    .getOrCreate()
)


# =====================================
# READ ALL SILVER TABLES
# =====================================
dim_location = spark.read.parquet(SILVER_PATH + "dim_location/")
dim_date = spark.read.parquet(SILVER_PATH + "dim_date/")
dim_time = spark.read.parquet(SILVER_PATH + "dim_time/")
dim_weather_condition = spark.read.parquet(SILVER_PATH + "dim_weather_condition/")
dim_alert = spark.read.parquet(SILVER_PATH + "dim_alert/")
fact_hourly = spark.read.parquet(SILVER_PATH + "fact_hourly_weather/")


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


# enrich month/year if needed
if "date" in df.columns:
    df = df.withColumn("month", month("date"))
    df = df.withColumn("year", year("date"))


# =====================================
# BUILD 7 GOLD FACTS
# =====================================
fact_weather_aggregate = build_fact_weather_aggregate(df)
fact_precip = build_fact_precipitation_analysis(df)
fact_extreme = build_fact_extreme_events(df)
fact_monthly = build_fact_monthly_climate_snapshot(df)
fact_weather_monthly = build_fact_weather_monthly(fact_precip)
fact_risk = build_fact_regional_risk_daily_snapshot(df)
fact_yearly = build_fact_extreme_event_yearly_snapshot(fact_extreme)


# =====================================
# REPUBLISH BASE STAR SCHEMA TO GOLD
# =====================================
dim_location.write.mode("overwrite").parquet(GOLD_PATH + "dim_location/")
dim_date.write.mode("overwrite").parquet(GOLD_PATH + "dim_date/")
dim_time.write.mode("overwrite").parquet(GOLD_PATH + "dim_time/")
dim_weather_condition.write.mode("overwrite").parquet(GOLD_PATH + "dim_weather_condition/")
dim_alert.write.mode("overwrite").parquet(GOLD_PATH + "dim_alert/")
fact_hourly.write.mode("overwrite").parquet(GOLD_PATH + "fact_hourly_observation/")


# =====================================
# WRITE 7 BUSINESS FACTS
# =====================================
fact_weather_aggregate.write.mode("overwrite").parquet(
    GOLD_PATH + "fact_weather_aggregate/"
)

fact_precip.write.mode("overwrite").parquet(
    GOLD_PATH + "fact_precipitation_analysis/"
)

fact_extreme.write.mode("overwrite").parquet(
    GOLD_PATH + "fact_extreme_events/"
)

fact_monthly.write.mode("overwrite").parquet(
    GOLD_PATH + "fact_monthly_climate_snapshot/"
)

fact_weather_monthly.write.mode("overwrite").parquet(
    GOLD_PATH + "fact_weather_monthly/"
)

fact_risk.write.mode("overwrite").parquet(
    GOLD_PATH + "fact_regional_risk_daily_snapshot/"
)

fact_yearly.write.mode("overwrite").parquet(
    GOLD_PATH + "fact_extreme_event_yearly_snapshot/"
)

print(" SILVER TO GOLD COMPLETED")