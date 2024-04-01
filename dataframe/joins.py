from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Joins") \
    .getOrCreate()

data1 = [("Anjali", 1),
         ("Akanksha", 2),
         ("Meera", 3)]
df1 = spark.createDataFrame(data1, ["name", "id"])

data2 = [("Abhishek", 4),
         ("Anjali", 5),
         ("Meera", 6)]
df2 = spark.createDataFrame(data2, ["name", "id"])

joined_df = df1.join(df2, "name", "inner")
joined_df.show()

spark.stop()
