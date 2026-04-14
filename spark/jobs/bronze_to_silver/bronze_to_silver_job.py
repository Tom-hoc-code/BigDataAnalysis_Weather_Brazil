from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import (
    col,
    dayofmonth,
    month,
    year,
    when,
    sha2,
    concat_ws
)
from pyspark import StorageLevel

from clean_data import clean_data
from standardize_data import standardize_data


# =====================================
# PATH + CATALOG
# =====================================
BRONZE_PATH = "s3a://s3-group2-bigdata/bronze/"
SILVER_PATH = "s3a://s3-group2-bigdata/silver/"
WareHouse_PATH = "s3a://s3-group2-bigdata/"

CATALOG = "weather_catalog"
SILVER_DB = "silver"

# data = [
#     ("obs_001","00",0.0,1012.5,1015.0,1010.0,0.0,22.5,18.0,25.0,20.0,19.5,17.0,85.0,60.0,72.0,180.0,15.0,8.0,"Sao Paulo","Station A","STA001",-23.55,-46.63,760.0,"file_01.csv","2023-01-01","Southeast","Mild","Normal","Moderate","None","Low"),
#     ("obs_002","01",0.5,1011.0,1013.5,1009.0,0.0,21.0,17.5,23.0,19.5,18.5,16.0,90.0,65.0,75.0,200.0,20.0,10.0,"Sao Paulo","Station A","STA001",-23.55,-46.63,760.0,"file_01.csv","2023-01-01","Southeast","Cool","High","Strong","Rain","Medium"),
# ]

# columns = [
#     "observation_id","hour","rainfall_hourly","pressure","pressure_max","pressure_min",
#     "solar_radiation","temperature","dew_point","temperature_max","temperature_min",
#     "dew_point_max","dew_point_min","humidity_max","humidity_min","humidity",
#     "wind_direction","wind_gust","wind_speed","state","station_name","station_code",
#     "latitude","longitude","elevation_m","source_file","date","region",
#     "temp_category","humidity_category","wind_level","alert_type","severity"
# ]


# =====================================
# SPARK + ICEBERG + GLUE
# =====================================
spark = (
    SparkSession.builder
    .appName("bronze_to_silver_weather_dw_iceberg")
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
        WareHouse_PATH
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.glue.region",
        "ap-southeast-2"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =====================================
# READ BRONZE
# =====================================
print("READ BRONZE DATA")
df = spark.read.parquet(BRONZE_PATH)
# df = spark.createDataFrame(data, columns)


# =====================================
# STANDARDIZE + CLEAN
# =====================================
print("START CLEANING + STANDARDIZATION")
df = standardize_data(df)
df = clean_data(df)

df = df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()


# =====================================
# CAST TYPES
# =====================================
print("CAST DATA TYPES")
df = (
    df
    .withColumn("date", col("date").cast("date"))
    # giữ hour là string để khớp DDL dim_date_time.hour VARCHAR
    .withColumn("hour", col("hour").cast("string"))
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn("elevation_m", col("elevation_m").cast("double"))
    .withColumn("rainfall_hourly", col("rainfall_hourly").cast("double"))
    .withColumn("pressure", col("pressure").cast("double"))
    .withColumn("pressure_max", col("pressure_max").cast("double"))
    .withColumn("pressure_min", col("pressure_min").cast("double"))
    .withColumn("solar_radiation", col("solar_radiation").cast("double"))
    .withColumn("temperature", col("temperature").cast("double"))
    .withColumn("temperature_max", col("temperature_max").cast("double"))
    .withColumn("temperature_min", col("temperature_min").cast("double"))
    .withColumn("dew_point", col("dew_point").cast("double"))
    .withColumn("dew_point_max", col("dew_point_max").cast("double"))
    .withColumn("dew_point_min", col("dew_point_min").cast("double"))
    .withColumn("humidity", col("humidity").cast("double"))
    .withColumn("humidity_max", col("humidity_max").cast("double"))
    .withColumn("humidity_min", col("humidity_min").cast("double"))
    .withColumn("wind_speed", col("wind_speed").cast("double"))
    .withColumn("wind_gust", col("wind_gust").cast("double"))
    .withColumn("wind_direction", col("wind_direction").cast("double"))
    .withColumn("severity", col("severity").cast("string"))
)


# =====================================
# DIM LOCATION
# Khớp DDL:
# location_key, station_code, region, state, latitude, longitude
# =====================================
print("BUILD DIM LOCATION")
dim_location = (
    df.select(
        "station_code",
        "region",
        "state",
        "latitude",
        "longitude"
    )
    .dropDuplicates()
    .withColumn(
        "location_key",
        sha2(
            concat_ws(
                "||",
                col("station_code"),
                col("state"),
                col("region"),
                col("latitude").cast("string"),
                col("longitude").cast("string")
            ),
            256
        )
    )
    .select(
        "location_key",
        "station_code",
        "region",
        "state",
        "latitude",
        "longitude"
    )
)


# =====================================
# DIM DATE TIME
# Khớp DDL:
# date_time_key, date, hour, day, month, year, season
# =====================================
print("BUILD DIM DATE TIME")
dim_date_time = (
    df.select("date", "hour")
    .dropDuplicates()
    .withColumn("day", dayofmonth("date"))
    .withColumn("month", month("date"))
    .withColumn("year", year("date"))
    .withColumn(
        "season",
        when(month("date").isin(12, 1, 2), "Winter")
        .when(month("date").isin(3, 4, 5), "Spring")
        .when(month("date").isin(6, 7, 8), "Summer")
        .otherwise("Autumn")
    )
    .withColumn(
        "date_time_key",
        sha2(
            concat_ws(
                "||",
                col("date").cast("string"),
                col("hour")
            ),
            256
        )
    )
    .select(
        "date_time_key",
        "date",
        "hour",
        "day",
        "month",
        "year",
        "season"
    )
)


# =====================================
# DIM WEATHER CONDITION
# Khớp DDL:
# condition_key, temp_category, humidity_category, wind_level
# =====================================
print("BUILD DIM WEATHER CONDITION")
dim_weather_condition = (
    df.select(
        "temp_category",
        "humidity_category",
        "wind_level"
    )
    .dropDuplicates()
    .withColumn(
        "condition_key",
        sha2(
            concat_ws(
                "||",
                col("temp_category"),
                col("humidity_category"),
                col("wind_level")
            ),
            256
        )
    )
    .select(
        "condition_key",
        "temp_category",
        "humidity_category",
        "wind_level"
    )
)


# =====================================
# DIM ALERT
# Khớp DDL
# =====================================
print("BUILD DIM ALERT")
dim_alert = (
    df.select(
        "alert_type",
        "severity"
    )
    .dropDuplicates()
    .withColumn(
        "alert_key",
        sha2(
            concat_ws(
                "||",
                col("alert_type"),
                col("severity")
            ),
            256
        )
    )
    .select("alert_key", "alert_type", "severity")
)


# =====================================
# FACT HOURLY OBSERVATION
# Khớp DDL:
# fact_key, observation_id, date_time_key, location_key, condition_key,
# alert_key, source_file, ...
# =====================================
print("BUILD FACT HOURLY OBSERVATION")
fact_hourly = (
    df
    .join(
        dim_location,
        [
            "station_code",
            "region",
            "state",
            "latitude",
            "longitude"
        ],
        "left"
    )
    .join(
        dim_date_time,
        ["date", "hour"],
        "left"
    )
    .join(
        dim_weather_condition,
        ["temp_category", "humidity_category", "wind_level"],
        "left"
    )
    .join(
        dim_alert,
        ["alert_type", "severity"],
        "left"
    )
    .withColumn(
        "fact_key",
        sha2(
            concat_ws(
                "||",
                col("observation_id"),
                col("date_time_key"),
                col("location_key")
            ),
            256
        )
    )
    .select(
        "fact_key",
        "observation_id",
        "date_time_key",
        "location_key",
        "condition_key",
        "alert_key",
        "source_file",
        "rainfall_hourly",
        "pressure",
        "pressure_max",
        "pressure_min",
        "solar_radiation",
        "temperature",
        "temperature_max",
        "temperature_min",
        "dew_point",
        "dew_point_max",
        "dew_point_min",
        "humidity",
        "humidity_max",
        "humidity_min",
        "wind_direction",
        "wind_speed",
        "wind_gust"
    )
)


# =====================================
# OPTIONAL DEBUG
# =====================================
print("DEBUG SAMPLE")
dim_location.show(3, False)
dim_date_time.show(3, False)
dim_weather_condition.show(3, False)
dim_alert.show(3, False)
fact_hourly.show(3, False)


# =====================================
# WRITE INTO EXISTING ICEBERG TABLES
# =====================================
print("WRITE INTO EXISTING ICEBERG TABLES")

# Dim tables của bạn không partition trong DDL
# nên dùng overwriteFiles() phù hợp hơn overwritePartitions()
dim_location.writeTo(f"{CATALOG}.{SILVER_DB}.dim_location").overwrite(lit(True))
dim_date_time.writeTo(f"{CATALOG}.{SILVER_DB}.dim_date_time").overwrite(lit(True))
dim_weather_condition.writeTo(f"{CATALOG}.{SILVER_DB}.dim_weather_condition").overwrite(lit(True))
dim_alert.writeTo(f"{CATALOG}.{SILVER_DB}.dim_alert").overwrite(lit(True))

# Fact table có partitioning bucket(location_key, 16)
fact_hourly.writeTo(f"{CATALOG}.{SILVER_DB}.fact_hourly_observation").overwrite(lit(True))

print("BRONZE TO SILVER ICEBERG LOAD COMPLETED")