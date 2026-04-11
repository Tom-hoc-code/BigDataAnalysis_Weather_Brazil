from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, when, col


def build_fact_precipitation_analysis(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("date_key", "location_key")
        .agg(
            sum("rainfall_hourly").alias("total_rainfall")
        )
        .withColumn(
            "drought_index",
            when(col("total_rainfall") < 20, 0.9)
            .when(col("total_rainfall") < 50, 0.5)
            .otherwise(0.1)
        )
        .withColumn(
            "flood_risk",
            when(col("total_rainfall") > 100, "high")
            .when(col("total_rainfall") > 50, "medium")
            .otherwise("low")
        )
    )