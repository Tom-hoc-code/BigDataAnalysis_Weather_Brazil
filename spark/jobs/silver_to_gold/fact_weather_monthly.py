from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, when, col


def build_fact_weather_monthly(df_precip: DataFrame) -> DataFrame:
    return (
        df_precip.groupBy("month", "location_key")
        .agg(
            avg("total_rainfall").alias("water_impact_score"),
            avg("drought_index").alias("drought_level")
        )
        .withColumnRenamed("month", "month_key")
    )
# input fact_precipitation_analysis