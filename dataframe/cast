from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CAST Function Example") \
    .getOrCreate()

data = [("Anjali", 21), ("Abhishek", 22), ("Meera", 23), ("Amit", 24)]

df = spark.createDataFrame(data, ["name", "age"])

print("Original DataFrame:")
df.show()

df = df.withColumn("age_str", col("age").cast("string"))

print("DataFrame with Converted Age:")
df.show()

spark.stop()
