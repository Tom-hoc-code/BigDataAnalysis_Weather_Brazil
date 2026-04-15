from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, sum, count, when, col, round


def build_fact_monthly_climate_snapshot(
    df: DataFrame
) -> DataFrame:
    baseline_temp = df.agg(
        avg("temperature").alias("baseline_temp")
    ).collect()[0]["baseline_temp"]

    monthly_df = (
        df
        .groupBy("region", "state", "month", "year")
        .agg(
            round(avg("temperature"), 1).alias("avg_temp_monthly"),
            round(sum("rainfall_hourly"), 1).alias("total_rainfall_monthly"),
            count(
                when(col("rainfall_hourly") > 0, True)
            ).alias("rainy_days_count")
        )
    )
    monthly_df = monthly_df.withColumn(
        "temp_anomaly",
        round(col("avg_temp_monthly") - baseline_temp, 3)
    )

    return monthly_df