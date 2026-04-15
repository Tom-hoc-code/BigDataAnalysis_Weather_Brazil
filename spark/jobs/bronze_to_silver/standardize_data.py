# =====================================
# Nguyen Huu Tam - 23133067
# =====================================
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, when, sha2, concat_ws, dayofmonth, month, year


# =========================================================
# COLUMN MAPPING
# =========================================================
COLUMN_MAPPING = {
    "index": "observation_id",
    "Data": "date",
    "Hora": "hour",

    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": "rainfall_hourly",

    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "pressure",
    "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)": "pressure_max",
    "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)": "pressure_min",

    "RADIACAO GLOBAL (Kj/m²)": "solar_radiation",

    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "temperature",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)": "dew_point",

    "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)": "temperature_max",
    "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)": "temperature_min",

    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)": "dew_point_max",
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)": "dew_point_min",

    "UMIDADE RELATIVA DO AR, HORARIA (%)": "humidity",
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)": "humidity_max",
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)": "humidity_min",

    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": "wind_direction",
    "VENTO, RAJADA MAXIMA (m/s)": "wind_gust",
    "VENTO, VELOCIDADE HORARIA (m/s)": "wind_speed",

    "state": "state",
    "station": "station_name",
    "station_code": "station_code",

    "latitude": "latitude",
    "longitude": "longitude",
    "height": "elevation_m",

    "source_file": "source_file",
    "parsed_date": "parsed_date",
    "region": "region"
}


# =========================================================
# BASIC CLEANING
# =========================================================
def remove_duplicate_columns(df: DataFrame) -> DataFrame:
    unique_cols = []
    seen = set()

    for c in df.columns:
        if c not in seen:
            unique_cols.append(c)
            seen.add(c)

    return df.select(*unique_cols)


def rename_columns(df: DataFrame) -> DataFrame:
    for old, new in COLUMN_MAPPING.items():
        if old in df.columns and old != new:
            df = df.withColumnRenamed(old, new)
    return df

def replace_date_with_parsed_date(df: DataFrame) -> DataFrame:

    # xóa date cũ nếu có
    if "date" in df.columns:
        df = df.drop("date")

    # đổi parsed_date -> date
    if "parsed_date" in df.columns:
        df = df.withColumnRenamed("parsed_date", "date")

    return df

def trim_whitespace(df: DataFrame) -> DataFrame:
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))
    return df


def lowercase_text(df: DataFrame) -> DataFrame:
    for c in ["state", "station_name", "region"]:
        if c in df.columns:
            df = df.withColumn(c, lower(col(c)))
    return df


def cast_datatypes(df: DataFrame) -> DataFrame:
    cast_map = {
        "latitude": "double",
        "longitude": "double",
        "elevation_m": "double",
        "rainfall_hourly": "double",
        "pressure": "double",
        "pressure_max": "double",
        "pressure_min": "double",
        "solar_radiation": "double",
        "temperature": "double",
        "temperature_max": "double",
        "temperature_min": "double",
        "dew_point": "double",
        "dew_point_max": "double",
        "dew_point_min": "double",
        "humidity": "double",
        "humidity_max": "double",
        "humidity_min": "double",
        "wind_direction": "double",
        "wind_gust": "double",
        "wind_speed": "double"
    }

    for c, t in cast_map.items():
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(t))

    return df


# =========================================================
# FEATURE ENGINEERING (IMPORTANT FIX)
# =========================================================
def add_features(df: DataFrame) -> DataFrame:

    # TEMP CATEGORY
    df = df.withColumn(
        "temp_category",
        when(col("temperature") < 10, "cold")
        .when((col("temperature") >= 10) & (col("temperature") < 25), "normal")
        .otherwise("hot")
    )

    # HUMIDITY CATEGORY
    df = df.withColumn(
        "humidity_category",
        when(col("humidity") < 40, "dry")
        .when((col("humidity") >= 40) & (col("humidity") < 70), "normal")
        .otherwise("humid")
    )

    # WIND LEVEL
    df = df.withColumn(
        "wind_level",
        when(col("wind_speed") < 3, "low")
        .when((col("wind_speed") >= 3) & (col("wind_speed") < 8), "medium")
        .otherwise("high")
    )

    # ALERT TYPE
    df = df.withColumn(
        "alert_type",
        when(col("temperature") > 40, "heatwave")
        .when(col("humidity") > 90, "storm_risk")
        .when(col("wind_speed") > 15, "windstorm")
        .otherwise("normal")
    )

    # SEVERITY
    df = df.withColumn(
        "severity",
        when(col("alert_type") == "normal", "low")
        .when(col("alert_type") == "windstorm", "medium")
        .otherwise("high")
    )

    return df

# =========================================================
# MAIN STANDARDIZATION PIPELINE
# =========================================================
def standardize_data(df: DataFrame) -> DataFrame:

    print("\n==== START STANDARDIZATION ====")

    df = rename_columns(df)
    df = replace_date_with_parsed_date(df)
    df = remove_duplicate_columns(df)
    df = trim_whitespace(df)
    df = lowercase_text(df)
    df = cast_datatypes(df)
    df = add_features(df)

    print("\nFINAL SCHEMA:")
    df.printSchema()

    return df