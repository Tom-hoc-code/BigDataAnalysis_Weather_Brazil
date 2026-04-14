from pyspark.sql import DataFrame
from pyspark.sql.functions import count, sum, avg, col, when, round


def build_fact_extreme_event_yearly_snapshot(
        df_date_time: DataFrame,
        df_extreme: DataFrame
) -> DataFrame:
    df_extreme = df_extreme.join(
        df_date_time.select("date_time_key", "year"),
        on="date_time_key",
        how="left"
    )

    df_extreme = df_extreme.withColumn(
        "severity_score",
        when(col("severity") == "low", 1)
        .when(col("severity") == "medium", 2)
        .when(col("severity") == "high", 3)
        .otherwise(0)
    )
    return (
        df_extreme
        .groupBy("year", "region","state")
        .agg(
            count("event_type").alias("extreme_event_count"),
            sum("duration").alias("total_event_duration"),
            round(avg(col("severity_score")), 2).alias("avg_severity")
        )
    )