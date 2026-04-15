# =====================================
# Nguyen Huu Tam - 23133067
# =====================================
from pyspark.sql.functions import col, when
from pyspark.sql import DataFrame

def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()


def remove_nulls(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["date", "hour", "station_code"])


def remove_negative_values(df: DataFrame) -> DataFrame:
    numeric_cols = [
        "rainfall_hourly",
        "pressure",
        "solar_radiation",
        "humidity",
        "wind_speed"
    ]

    for col_name in numeric_cols:
        df = df.filter(
            (col(col_name).cast("double").isNull()) |
            (col(col_name).cast("double") >= 0)
        )

    return df


def remove_outliers(df: DataFrame) -> DataFrame:
    # nhiệt độ hợp lý
    df = df.filter(
        (col("temperature").cast("double") >= -20) &
        (col("temperature").cast("double") <= 60)
    )

    # độ ẩm
    df = df.filter(
        (col("humidity").cast("double") >= 0) &
        (col("humidity").cast("double") <= 100)
    )

    return df


def clean_data(df: DataFrame) -> DataFrame:
    print("\n" + "=" * 60)
    print("START CLEANING DATA")
    print("=" * 60)
    df = remove_duplicates(df)
    df = remove_nulls(df)
    df = remove_negative_values(df)
    df = remove_outliers(df)
    return df