from pyspark.sql import DataFrame
from pyspark.sql.functions import count, sum, avg


def build_fact_extreme_event_yearly_snapshot(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("year", "location_key")
        .agg(
            count("event_type").alias("extreme_event_count"),
            sum("duration").alias("total_event_duration"),
            avg("severity").alias("avg_severity")
        )
        .withColumnRenamed("year", "year_key")
    )

# input build_fact_extreme_events