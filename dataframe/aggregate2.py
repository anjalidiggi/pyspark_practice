from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .appName("Numeric Functions Example") \
    .getOrCreate()

data = [("Anjali", 100),
        ("Akanksha", 150),
        ("Meera", 200),
        ("Abhishek", 250)]
df = spark.createDataFrame(data, ["name", "amount"])

df.withColumn("increased_amount", col("amount") * lit(1.1)).show()

spark.stop()
