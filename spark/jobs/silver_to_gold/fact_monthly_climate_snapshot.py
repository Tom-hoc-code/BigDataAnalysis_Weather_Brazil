from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    sum,
    count,
    when,
    col
)


def build_fact_monthly_climate_snapshot(df: DataFrame) -> DataFrame:
    """
    Build monthly climate KPI snapshot
    Grain: month + location
    """

    # baseline temperature toàn dataset
    baseline_temp = df.agg(
        avg("temperature").alias("baseline_temp")
    ).collect()[0]["baseline_temp"]

    monthly_df = (
        df.groupBy("month", "location_key")
        .agg(
            avg("temperature").alias("avg_temp_monthly"),
            sum("rainfall_hourly").alias("total_rainfall_monthly"),
            count(
                when(col("rainfall_hourly") > 0, True)
            ).alias("rainy_days_count")
        )
    )

    # temp anomaly = avg tháng - avg toàn dataset
    monthly_df = monthly_df.withColumn(
        "temp_anomaly",
        col("avg_temp_monthly") - baseline_temp
    )

    return monthly_df