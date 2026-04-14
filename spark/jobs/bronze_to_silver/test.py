from pyspark.sql import SparkSession

WAREHOUSE_PATH = "s3a://s3-group2-bigdata/warehouse/"

spark = (
    SparkSession.builder
    .appName("weather_lakehouse")

    # Iceberg extension (BẮT BUỘC)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # catalog name
    .config(
        "spark.sql.catalog.weather_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # dùng AWS Glue
    .config(
        "spark.sql.catalog.weather_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    # IO S3
    .config(
        "spark.sql.catalog.weather_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    # nơi lưu table files
    .config(
        "spark.sql.catalog.weather_catalog.warehouse",
        WAREHOUSE_PATH
    )

    # region
    .config(
        "spark.sql.catalog.weather_catalog.glue.region",
        "ap-southeast-2"
    )

    .getOrCreate()
)

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS
weather_catalog.weather_silver_test
""")
print(spark.conf.get("spark.sql.catalog.weather_catalog"))

spark.sql("""
SHOW NAMESPACES IN weather_catalog
""").show(truncate=False)

spark.sql("""
SHOW TABLES IN weather_catalog.weather_silver
""").show(truncate=False)

spark.sql("""
DESCRIBE EXTENDED
weather_catalog.weather_silver.fact_hourly_weather
""").show(truncate=False)