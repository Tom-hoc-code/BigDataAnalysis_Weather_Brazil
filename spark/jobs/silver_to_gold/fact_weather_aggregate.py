from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, min, max, sum, first, round


def build_fact_weather_aggregate(df):
    return (
        df.groupBy("date_time_key", "region", "state")
        .agg(
            round(avg("temperature"), 2).alias("avg_temp"),
            max("temperature_max").alias("max_temp"),
            min("temperature_min").alias("min_temp"),
            round(sum("rainfall_hourly"), 2).alias("total_rainfall"),
            round(avg("wind_speed"), 2).alias("wind_speed"),
            max("wind_gust").alias("max_gust"),
            round(first("wind_direction"), 2).alias("dominant_direction")
        )
    )