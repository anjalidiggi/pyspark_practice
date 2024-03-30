from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, avg, collect_list, collect_set, countDistinct, count, first, last, max, min, sum

spark = SparkSession.builder.appName("FunctionsDemo").getOrCreate()

data = [("Anjali",), ("Abhishek",), ("Meera",), ("Amit",)]
columns = ["name"]
df = spark.createDataFrame(data, columns)

df_avg = df.select(avg("name")).show()

spark.stop()
