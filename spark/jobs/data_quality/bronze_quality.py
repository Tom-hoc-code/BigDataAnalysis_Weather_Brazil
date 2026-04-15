# =====================================
# Luong Ngoc Huy - 23133028
# =====================================
from pyspark.sql import DataFrame, SparkSession
import re
import unicodedata

# =========================================================
# COUNT RECORDS
# =========================================================
def get_total_records(df: DataFrame) -> int:
    return df.count()


# =========================================================
# ULTRA LIGHT DATA CHECK
# =========================================================
def data_quality_report_light(
    df: DataFrame,
    count_records: bool = False,
    fast_count: bool = True
) -> None:
    """
    Check:
    - Tổng data
    - columns
    - schema
    - dtypes
    - partitions
    - input files
    - execution plan
    """

    print("=" * 90)
    print("ULTRA LIGHT DATA CHECK")
    print("=" * 90)

    # -----------------------------------------------------
    # STRUCTURE
    # -----------------------------------------------------
    print("\nCOLUMNS:")
    for c in df.columns:
        print(f"  - {c}")

    print("\nSCHEMA:")
    df.printSchema()

    print("\nDTYPES:")
    for col_name, dtype in df.dtypes:
        print(f"  - {col_name}: {dtype}")

    print("\nPARTITIONS:")
    partitions = df.rdd.getNumPartitions()
    print(f"  - {partitions}")

    # -----------------------------------------------------
    # FILE METADATA
    # -----------------------------------------------------
    print("\nINPUT FILES (first 10):")
    input_files = df.inputFiles()

    if input_files:
        for f in input_files[:10]:
            print(f"  - {f}")
        if len(input_files) > 10:
            print(f"  ... and {len(input_files) - 10} more files")
    else:
        print("  - No input files found")


    # -----------------------------------------------------
    # OPTIONAL COUNT
    # -----------------------------------------------------
    total_records = None
    if count_records:
        total_records = get_total_records(df)

    # -----------------------------------------------------
    # SUMMARY
    # -----------------------------------------------------
    print("\n" + "=" * 90)
    print("SUMMARY")
    print("=" * 90)

    print(f"  - Total columns   : {len(df.columns)}")
    print(f"  - Partitions      : {partitions}")
    print(f"  - Input files     : {len(input_files)}")

    if total_records is not None:
        print(f"  - Total records   : {total_records:,}")



# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.parquet("s3a://s3-group2-bigdata/bronze/")

    # chạy report
    data_quality_report_light(
        df,
        count_records=True,   
    )