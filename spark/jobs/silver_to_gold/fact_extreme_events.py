from pyspark.sql import DataFrame
from pyspark.sql.functions import when, lit


def build_fact_extreme_events(df: DataFrame) -> DataFrame:
    return (
        df.filter(df.alert_type != "normal")
        .select(
            "date_key",
            "location_key",
            "alert_type",
            "severity"
        )
        .withColumn("duration", lit(1))
        .withColumnRenamed("alert_type", "event_type")
    )