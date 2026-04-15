# =====================================
# Nguyen Huu Tam - 23133067
# =====================================
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg

def build_fact_weather_monthly(
    df_precip: DataFrame,
    dim_date_time: DataFrame
) -> DataFrame:
    df_joined = df_precip.join(
        dim_date_time.select("date_time_key", "month", "year"),
        on="date_time_key",
        how="left"
    )

    return (
        df_joined
        .groupBy("date_time_key", "region", "state", "month", "year")
        .agg(
            avg("total_rainfall").alias("avg_rainfall"),
            avg("drought_index").alias("drought_level")
        )
    )