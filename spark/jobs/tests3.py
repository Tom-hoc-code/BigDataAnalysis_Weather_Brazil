from pyspark.sql import SparkSession

BRONZE_PATH = "s3a://s3-group2-bigdata/bronze/region=CO"

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet(BRONZE_PATH)
print("Schema / Data types:")
df.printSchema()

# print("Top 5 rows:")
# df.show(5, truncate=False)

# spark-submit --master spark://gr2-spark-master:7077 /app/jobs/bronze_to_silver/bronze_to_silver_job.py
# spark-submit --master spark://gr2-spark-master:7077 /app/jobs/silver_to_gold/silver_to_gold_job.py