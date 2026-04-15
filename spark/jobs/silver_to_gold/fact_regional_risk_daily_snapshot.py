from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, count, max, round

def build_fact_regional_risk_daily_snapshot(df: DataFrame) -> DataFrame:
    """
    Daily regional risk snapshot
    risk_index_score = severity_score * cumulative_alert_duration
    """

    df = df.withColumn(
        "severity_score",
        when(col("severity") == "low", 1)
        .when(col("severity") == "medium", 2)
        .when(col("severity") == "high", 3)
        .otherwise(0)
    )

    return (
        df
        .groupBy(
            "day",
            "month",
            "year",
            "region",
            "state",
            "alert_type"
        )
        .agg(
            count(
                when(col("alert_type").isNotNull(), True)
            ).alias("cumulative_alert_duration"),
            max("severity_score").alias("severity_score")
        )
        .withColumn(
            "risk_index_score",
            round(col("cumulative_alert_duration") * col("severity_score"), 2)
        )
        .select(
            "day",
            "month",
            "year",
            "region",
            "state",
            "alert_type",
            "cumulative_alert_duration",
            "risk_index_score"
        )
    )