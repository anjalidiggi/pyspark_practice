from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("JoinsExample").getOrCreate()

data1 = [("Abhishek", 100), ("Amit", 200), ("Anjali", 300)]
data2 = [("Ayush", 400), ("Abhinav", 500), ("Ayansh", 600)]

schema = ["name", "amount"]

df1 = spark.createDataFrame(data1, schema=schema)
df2 = spark.createDataFrame(data2, schema=schema)

joined_df = df1.join(df2, on="name", how="inner")

joined_df.show()

spark.stop()
