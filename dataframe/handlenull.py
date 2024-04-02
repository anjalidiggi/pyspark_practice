from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

spark = SparkSession.builder.appName("HandleNullAndDuplicates").getOrCreate()

data = [
    ("Abhishek", 100),
    ("Amit", 200),
    ("Anjali", None),
    ("Ayush", 400),
    ("Abhinav", 500),
    ("Ayansh", None),
    ("Abhishek", 600),
    ("Amit", 700),
    ("Anjali", 800),
    ("Ayush", None),
    ("Abhinav", 1100),
    ("Ayansh", 1200)
]

schema = ["name", "amount"]

df = spark.createDataFrame(data, schema=schema)

df = df.fillna({"amount": 0, "name": "Unknown"})

df = df.dropDuplicates()

df.show()

spark.stop()
