from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("Aggregate Functions Example") \
    .getOrCreate()

data = [("Anjali", 100),
        ("Akanksha", 150),
        ("Meera", 200),
        ("Abhishek", 250)]
df = spark.createDataFrame(data, ["name", "amount"])

avg_amount = df.agg(avg("amount").alias("avg_amount"))
avg_amount.show()

spark.stop()
