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
# PATH + CATALOG
# =====================================
BRONZE_PATH = "s3a://s3-group2-bigdata/bronze/"
SILVER_PATH = "s3a://s3-group2-bigdata/silver/"
CATALOG = "weather_catalog.weather_silver"


# =====================================
# SPARK + ICEBERG
# =====================================
spark = (
    SparkSession.builder
    .appName("bronze_to_silver_weather_dw")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.memoryOverhead", "1g")
    .config("spark.sql.shuffle.partitions", "120")
    .config("spark.sql.adaptive.enabled", "true")
    .config(
        "spark.sql.catalog.weather_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.weather_catalog.type", "hadoop")
    .config(
        "spark.sql.catalog.weather_catalog.warehouse",
        SILVER_PATH
    )
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS weather_catalog.weather_silver")


# =====================================
# HELPER SAVE ICEBERG
# =====================================
def save_iceberg(df, table_name):
    (
        df.writeTo(f"{CATALOG}.{table_name}")
        .createOrReplace()
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

df = df.withColumn("date", col("date").cast("date"))


# =====================================
# DIM LOCATION
# =====================================
dim_location = (
    df.select(
        "station_code",
        "station_name",
        "state",
        "region"
    )
    .dropDuplicates()
    .withColumn(
        "location_key",
        sha2(
            concat_ws(
                "||",
                col("station_code"),
                col("state"),
                col("region")
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
# Grain = station + date + hour
# =====================================
fact_hourly = (
    df
    .join(
        dim_location,
        ["station_code", "station_name", "state", "region"],
        "left"
    )
    .join(dim_date, "date", "left")
    .join(dim_time, "hour", "left")
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
        "condition_key",
        "alert_key",

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
# SAVE ICEBERG TABLES
# =====================================
save_iceberg(dim_location, "dim_location")
save_iceberg(dim_date, "dim_date")
save_iceberg(dim_time, "dim_time")
save_iceberg(dim_weather_condition, "dim_weather_condition")
save_iceberg(dim_alert, "dim_alert")
save_iceberg(fact_hourly, "fact_hourly_weather")


print("✅ BRONZE TO SILVER COMPLETED")