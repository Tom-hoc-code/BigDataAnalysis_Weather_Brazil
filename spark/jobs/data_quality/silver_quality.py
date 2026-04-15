# =====================================
# Luong Ngoc Huy - 23133028
# =====================================
from pyspark.sql import DataFrame, SparkSession
import re
import unicodedata

def get_total_records(df: DataFrame) -> int:
    return df.count()


# =========================================================
# ULTRA LIGHT DATA CHECK
# =========================================================
def data_quality_report_light(
    df: DataFrame,
    dataset_name: str,
    count_records: bool = False
) -> None:
    print("=" * 90)
    print(f"DATA QUALITY CHECK - {dataset_name}")
    print("=" * 90)

    print("\nCOLUMNS:")
    for c in df.columns:
        print(f"  - {c}")

    print("\nSCHEMA:")
    df.printSchema()

    print("\nDTYPES:")
    for col_name, dtype in df.dtypes:
        print(f"  - {col_name}: {dtype}")

    partitions = df.rdd.getNumPartitions()
    print("\nPARTITIONS:")
    print(f"  - {partitions}")

    input_files = df.inputFiles()
    print("\nINPUT FILES (first 10):")
    if input_files:
        for f in input_files[:10]:
            print(f"  - {f}")
        if len(input_files) > 10:
            print(f"  ... and {len(input_files) - 10} more files")
    else:
        print("  - No input files found")

    total_records = None
    if count_records:
        total_records = get_total_records(df)

    print("\n" + "=" * 90)
    print("SUMMARY")
    print("=" * 90)
    print(f"  - Table         : {dataset_name}")
    print(f"  - Total columns   : {len(df.columns)}")
    print(f"  - Partitions      : {partitions}")
    print(f"  - Input files     : {len(input_files)}")

    if total_records is not None:
        print(f"  - Total records   : {total_records:,}")

    print("Hoàn thành data quality check")
    print()



# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    SILVER_PATH = "s3a://s3-group2-bigdata/silver/"
    WareHouse_PATH = "s3a://s3-group2-bigdata/"
    CATALOG = "weather_catalog"
    SILVER_DB = "silver"

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

    # Chuyển current catalog sang iceberg
    spark.sql("USE weather_catalog")

    # Lấy danh sách bảng trong namespace silver
    tables_df = spark.sql("SHOW TABLES IN silver")

    silver_tables = []
    for row in tables_df.collect():
        row_dict = row.asDict()

        table_name = row_dict.get("tableName") or row_dict.get("tablename")
        is_temporary = row_dict.get("isTemporary", False)

        if table_name and not is_temporary:
            silver_tables.append(f"weather_catalog.silver.{table_name}")

    print("Found Iceberg tables in weather_catalog.silver:")
    for table_name in silver_tables:
        print(f"  - {table_name}")

    for table_name in silver_tables:
        dataset_name = table_name.split(".")[-1]

        print("\n" + "#" * 90)
        print(f"READING ICEBERG TABLE: {table_name}")
        print("#" * 90)

        try:
            df = spark.read.table(table_name)

            data_quality_report_light(
                df,
                dataset_name=dataset_name,
                count_records=True,
            )

        except Exception as e:
            print(f"Skip table {table_name} because read failed: {e}")