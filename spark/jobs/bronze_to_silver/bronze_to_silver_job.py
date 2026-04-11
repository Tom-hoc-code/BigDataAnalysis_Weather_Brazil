from pyspark.sql import SparkSession
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
# PATH
# =====================================
BRONZE_PATH = "s3a://s3-group2-bigdata/bronze/"
SILVER_PATH = "s3a://s3-group2-bigdata/silver/"


# =====================================
# SPARK CONFIG (16GB OPTIMIZED)
# =====================================
spark = (
    SparkSession.builder
    .appName("bronze_to_silver_weather_dw")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.memoryOverhead", "1g")
    .config("spark.sql.shuffle.partitions", "120")
    .config("spark.default.parallelism", "120")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)


# =====================================
# READ BRONZE
# =====================================
df = spark.read.parquet(BRONZE_PATH)

# df = df.filter(col("region") == "CO")
# =====================================
# STANDARDIZE + CLEAN
# =====================================
df = standardize_data(df)
df = clean_data(df)

df = df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()


# =====================================
# ENSURE DATE TYPE
# =====================================
df = df.withColumn("date", col("date").cast("date"))

# =====================================
# DIM LOCATION
# =====================================
dim_location = (
    df.select(
        "station_code",
        "station_name",
        "state",
        "region",
        "latitude",
        "longitude",
        "elevation_m"
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
)

# =====================================
# DIM DATE
# =====================================
dim_date = (
    df.select("date")
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
        "date_key",
        sha2(col("date").cast("string"), 256)
    )
)

# =====================================
# DIM TIME
# =====================================
dim_time = (
    df.select("hour")
    .dropDuplicates()
    .withColumn(
        "time_key",
        sha2(col("hour").cast("string"), 256)
    )
)
# =====================================
# DIM WEATHER CONDITION
# =====================================
dim_weather_condition = (
    df.select(
        "temperature",
        "humidity",
        "wind_speed"
    )
    .dropDuplicates()
    .withColumn(
        "condition_key",
        sha2(
            concat_ws(
                "||",
                col("temperature"),
                col("humidity"),
                col("wind_speed")
            ),
            256
        )
    )
)

# =====================================
# DIM ALERT
# =====================================
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
)
# =====================================
# FACT HOURLY WEATHER
# Grain = 1 station + 1 date + 1 hour
# =====================================
fact_hourly = (
    df
    .join(
        dim_location,
        [
            "station_code",
            "station_name",
            "state",
            "region",
            "latitude",
            "longitude",
            "elevation_m"
        ],
        "left"
    )
    .join(dim_date, "date", "left")
    .join(dim_time, "hour", "left")
    .withColumn(
        "fact_key",
        sha2(
            concat_ws(
                "||",
                col("location_key"),
                col("date_key"),
                col("time_key")
            ),
            256
        )
    )
    .select(
        "fact_key",
        "date_key",
        "time_key",
        "location_key",
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
# DEBUG SAMPLE
# =====================================
dim_location.show(3, False)
dim_date.show(3, False)
dim_time.show(3, False)
dim_weather_condition.show(3, False)
dim_alert.show(3, False)
fact_hourly.show(3, False)


# =====================================
# WRITE SILVER
# =====================================
dim_location.write.mode("overwrite").parquet(SILVER_PATH + "dim_location/")
dim_date.write.mode("overwrite").parquet(SILVER_PATH + "dim_date/")
dim_time.write.mode("overwrite").parquet(SILVER_PATH + "dim_time/")
dim_weather_condition.write.mode("overwrite").parquet(SILVER_PATH + "dim_weather_condition/")
dim_alert.write.mode("overwrite").parquet(SILVER_PATH + "dim_alert/")

fact_hourly.write \
    .mode("overwrite") \
    .partitionBy("date_key") \
    .option("maxRecordsPerFile", 500000) \
    .parquet(SILVER_PATH + "fact_hourly_weather/")


print(" WEATHER STAR SCHEMA ETL COMPLETED")