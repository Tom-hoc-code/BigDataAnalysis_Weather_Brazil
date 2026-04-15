# =====================================
# Nguyen Huu Tam - 23133067
# =====================================
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, when, round

def build_fact_precipitation_analysis(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("date_time_key", "region", "state")
        .agg(
            round(sum("rainfall_hourly"), 2).alias("total_rainfall")
        )
        .withColumn(
            "drought_index",
            round(
                when(col("total_rainfall") < 20, 0.9)
                .when(col("total_rainfall") < 50, 0.5)
                .otherwise(0.1),
                2
            )
        )
        .withColumn(
            "flood_risk",
            round(
                when(col("total_rainfall") > 100, 1)
                .when(col("total_rainfall") > 50, 0.5)
                .otherwise(0),
                2
            )
        )
    )