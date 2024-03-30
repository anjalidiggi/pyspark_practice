from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CAST Function Example") \
    .getOrCreate()

data = [("1",), ("2",), ("3",), ("4",)]

df = spark.createDataFrame(data, ["number_str"])

print("Original DataFrame:")
df.show()


df = df.withColumn("number_int", col("number_str").cast("int"))

print("DataFrame with Converted Column:")
df.show()


spark.stop()
