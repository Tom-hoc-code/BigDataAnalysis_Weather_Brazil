from pyspark.sql import DataFrame
from pyspark.sql.functions import count, when, col, avg


def build_fact_regional_risk_daily_snapshot(df: DataFrame) -> DataFrame:
    """
    Daily regional risk snapshot
    risk_index_score = severity_score * cumulative_alert_duration
    """

    severity_score = (
        when(col("severity") == "low", 1)
        .when(col("severity") == "medium", 2)
        .when(col("severity") == "high", 3)
        .otherwise(0)
    )

    return (
        df.groupBy("date_key", "location_key", "alert_key", "severity")
        .agg(
            count(
                when(col("alert_key").isNotNull(), True)
            ).alias("cumulative_alert_duration")
        )
        .withColumn(
            "risk_index_score",
            col("cumulative_alert_duration") *
            severity_score
        )
    )