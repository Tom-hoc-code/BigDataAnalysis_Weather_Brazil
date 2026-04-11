# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col,
#     dayofmonth,
#     month,
#     year,
#     hour,
#     when,
#     monotonically_increasing_id,
#     broadcast
# )

# from clean_data import clean_data
# from standardize import standardize_data

# BRONZE_PATH = "s3a://s3-group2-bigdata/bronze/"
# SILVER_PATH = "s3a://s3-group2-bigdata/silver/"

# # =====================================
# # Spark Session (optimized for 16GB RAM)
# # =====================================
# spark = (
#     SparkSession.builder
#     .appName("bronze_to_silver")
#     .config("spark.executor.memory", "3g")
#     .config("spark.driver.memory", "3g")
#     .config("spark.sql.shuffle.partitions", "32")
#     .config("spark.default.parallelism", "16")
#     .getOrCreate()
# )

# # =====================================
# # Read Bronze
# # =====================================
# df = (
#     spark.read.parquet(BRONZE_PATH)
#     .repartition(16)
# )

# # =====================================
# # ETL pipeline
# # =====================================
# df = standardize_data(df)
# df = clean_data(df)

# # cache vì dùng nhiều lần
# df = df.cache()
# df.limit(1).count()

# # =====================================
# # 1. dim_location
# # =====================================
# dim_location = (
#     df.select(
#         "station_code",
#         "region",
#         "state",
#         "latitude",
#         "longitude"
#     )
#     .dropDuplicates()
#     .withColumn("location_key", monotonically_increasing_id())
#     .select(
#         "location_key",
#         "station_code",
#         "region",
#         "state",
#         "latitude",
#         "longitude"
#     )
# )

# print("===== dim_location =====")
# dim_location.printSchema()
# dim_location.limit(5).show(truncate=False)

# # dim_location.write.mode("overwrite").parquet(
# #     SILVER_PATH + "dim_location/"
# # )

# # =====================================
# # 2. dim_date_time
# # =====================================
# dim_date_time = (
#     df.select("observation_time")
#     .dropDuplicates()
#     .withColumn("date_key", monotonically_increasing_id())
#     .withColumn("date", col("observation_time").cast("date"))
#     .withColumn("day", dayofmonth("observation_time"))
#     .withColumn("month", month("observation_time"))
#     .withColumn("year", year("observation_time"))
#     .withColumn("hour", hour("observation_time"))
#     .withColumn(
#         "season",
#         when(month("observation_time").isin(12, 1, 2), "Winter")
#         .when(month("observation_time").isin(3, 4, 5), "Spring")
#         .when(month("observation_time").isin(6, 7, 8), "Summer")
#         .otherwise("Autumn")
#     )
#     .select(
#         "date_key",
#         "date",
#         "day",
#         "month",
#         "year",
#         "hour",
#         "season"
#     )
# )

# print("===== dim_date_time =====")
# dim_date_time.printSchema()
# dim_date_time.limit(5).show(truncate=False)

# # dim_date_time.write.mode("overwrite").parquet(
# #     SILVER_PATH + "dim_date_time/"
# # )

# # =====================================
# # 3. dim_weather_condition
# # =====================================
# dim_weather_condition = (
#     df.select(
#         "temp_category",
#         "humidity_category",
#         "wind_level"
#     )
#     .dropDuplicates()
#     .withColumn("condition_key", monotonically_increasing_id())
#     .select(
#         "condition_key",
#         "temp_category",
#         "humidity_category",
#         "wind_level"
#     )
# )

# print("===== dim_weather_condition =====")
# dim_weather_condition.printSchema()
# dim_weather_condition.limit(5).show(truncate=False)

# # dim_weather_condition.write.mode("overwrite").parquet(
# #     SILVER_PATH + "dim_weather_condition/"
# # )

# # =====================================
# # 4. dim_alert
# # =====================================
# dim_alert = (
#     df.select(
#         "alert_type",
#         "severity"
#     )
#     .dropDuplicates()
#     .withColumn("alert_key", monotonically_increasing_id())
#     .select(
#         "alert_key",
#         "alert_type",
#         "severity"
#     )
# )

# print("===== dim_alert =====")
# dim_alert.printSchema()
# dim_alert.limit(5).show(truncate=False)

# # dim_alert.write.mode("overwrite").parquet(
# #     SILVER_PATH + "dim_alert/"
# # )

# # =====================================
# # 5. fact_hourly_observation
# # =====================================
# fact_hourly = (
#     df.join(
#         broadcast(dim_location),
#         ["station_code", "region", "state", "latitude", "longitude"],
#         "left"
#     )
#     .join(
#         broadcast(dim_date_time),
#         df["observation_time"].cast("date") == dim_date_time["date"],
#         "left"
#     )
#     .join(
#         broadcast(dim_weather_condition),
#         ["temp_category", "humidity_category", "wind_level"],
#         "left"
#     )
#     .join(
#         broadcast(dim_alert),
#         ["alert_type", "severity"],
#         "left"
#     )
#     .withColumn("observation_id", monotonically_increasing_id())
#     .select(
#         "observation_id",
#         "date_key",
#         "location_key",
#         "condition_key",
#         "alert_key",
#         "rainfall_hourly",
#         "pressure",
#         "pressure_max",
#         "pressure_min",
#         "solar_radiation",
#         "temperature",
#         "temperature_max",
#         "temperature_min",
#         "dew_point",
#         "dew_point_max",
#         "dew_point_min",
#         "humidity",
#         "humidity_max",
#         "humidity_min",
#         "wind_direction",
#         "wind_speed",
#         "wind_gust"
#     )
# )

# print("===== fact_hourly =====")
# fact_hourly.printSchema()
# fact_hourly.limit(10).show(truncate=False)

# # fact_hourly.write.mode("overwrite").parquet(
# #     SILVER_PATH + "fact_hourly_observation/"
# # )

# print("✅ Load to Silver completed successfully")
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
from standardize import standardize_data


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

df = df.filter(col("region") == "CO")
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
# dim_location.write.mode("overwrite").parquet(SILVER_PATH + "dim_location/")
# dim_date.write.mode("overwrite").parquet(SILVER_PATH + "dim_date/")
# dim_time.write.mode("overwrite").parquet(SILVER_PATH + "dim_time/")
# dim_weather_condition.write.mode("overwrite").parquet(SILVER_PATH + "dim_weather_condition/")
# dim_alert.write.mode("overwrite").parquet(SILVER_PATH + "dim_alert/")
#
# fact_hourly.write \
#     .mode("overwrite") \
#     .partitionBy("date_key") \
#     .option("maxRecordsPerFile", 500000) \
#     .parquet(SILVER_PATH + "fact_hourly_weather/")


print("✅ WEATHER STAR SCHEMA ETL COMPLETED")