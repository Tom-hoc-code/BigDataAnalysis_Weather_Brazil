from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, min, max, sum, first


def build_fact_weather_aggregate(df):
    return (
        df.groupBy("date_key", "location_key")
        .agg(
            avg("temperature").alias("avg_temp"),
            max("temperature_max").alias("max_temp"),
            min("temperature_min").alias("min_temp"),
            sum("rainfall_hourly").alias("total_rainfall"),
            avg("wind_speed").alias("wind_speed"),
            max("wind_gust").alias("max_gust"),
            first("wind_direction").alias("dominant_direction")
        )
    )