# =====================================
# Nguyen Duc Thinh - 23133073
# =====================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    to_date,
    input_file_name,
    trim,
    regexp_replace
)

# ==============================
# CONFIG
# ==============================
SOURCE_PATH = "s3a://s3-group2-bigdata/datasource/*.csv"
BRONZE_PATH = "s3a://s3-group2-bigdata/bronze/"
QUARANTINE_PATH = "s3a://s3-group2-bigdata/quarantine/"

spark = SparkSession.builder.getOrCreate()

print("=============[INGESTION_START] reading CSV files from S3 source=============")

# ==============================
# EXTRACT FROM S3
# ==============================
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(SOURCE_PATH)
)

# thêm thông tin file nguồn để trace
df = df.withColumn("source_file", input_file_name())

input_count = df.count()
print(f"=============[EXTRACT_DONE] input_count={input_count}=============")

# ==============================
# CLEAN + DATE HANDLING
# ==============================
df = (
    df.withColumn("Data", trim(regexp_replace(col("Data"), r"[\r\n\t]", "")))
      .withColumn("region", trim(col("region")))
      .withColumn("parsed_date", to_date(col("Data"), "yyyy-MM-dd"))
)

print("=============[TRANSFORM_DONE] cleaned_columns=Data,region date_format=yyyy-MM-dd=============")

# ==============================
# DATA QUALITY CHECKS
# ==============================
valid_df = df.filter(
    col("parsed_date").isNotNull() &
    col("region").isNotNull() &
    (col("region") != "")
)

invalid_df = (
    df.filter(
        col("parsed_date").isNull() |
        col("region").isNull() |
        (col("region") == "")
    )
    .withColumn("error_reason", lit("invalid_or_missing_required_fields"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

valid_count = valid_df.count()
invalid_count = invalid_df.count()

print(f"=============[QUALITY_DONE] valid={valid_count} invalid={invalid_count}=============")

# ==============================
# LOAD TO BRONZE AS PARQUET
# PARTITION BY REGION
# ==============================
(
    valid_df.write
    .mode("append")
    .partitionBy("region")
    .parquet(BRONZE_PATH)
)

print(f"=============[BRONZE_WRITE_DONE] path={BRONZE_PATH}=============")

# ==============================
# WRITE QUARANTINE
# ==============================
(
    invalid_df.write
    .mode("append")
    .parquet(QUARANTINE_PATH)
)

print(f"=============[QUARANTINE_WRITE_DONE] path={QUARANTINE_PATH}=============")
print("=============[INGESTION_SUCCESS]=============")