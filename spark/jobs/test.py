from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.lakehouse.test_table (
  id BIGINT,
  name STRING
) USING iceberg
""")

spark.sql("""
INSERT INTO glue_catalog.lakehouse.test_table VALUES
(1, 'Alice'),
(2, 'Bob')
""")

spark.sql("SELECT * FROM glue_catalog.lakehouse.test_table").show()