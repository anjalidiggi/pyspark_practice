from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowFunctionsExample").getOrCreate()

data = [
    ("Abhishek", 100),
    ("Amit", 200),
    ("Anjali", 300),
    ("Ayush", 400),
    ("Abhinav", 500),
    ("Ayansh", 600),
    ("Abhishek", 700),
    ("Amit", 800),
    ("Anjali", 900),
    ("Ayush", 1000),
    ("Abhinav", 1100),
    ("Ayansh", 1200)
]

schema = ["name", "amount"]

df = spark.createDataFrame(data, schema=schema)

windowSpec = Window.partitionBy().orderBy(col("amount").desc())

df = df.withColumn("rank", rank().over(windowSpec))

df.show()

spark.stop()
