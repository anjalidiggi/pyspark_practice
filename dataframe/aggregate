from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, avg, collect_list, collect_set, countDistinct, count, first, last, max, min, sum

spark = SparkSession.builder.appName("Functions").getOrCreate()

data = [("Anjali",), ("Abhishek",), ("Meera",), ("Amit",)]
columns = ["name"]
df = spark.createDataFrame(data, columns)

df_collect_set = df.select(collect_set("name")).show()

df_count_distinct = df.select(countDistinct("name")).show()

df_count = df.select(count("name")).show()

df_first = df.select(first("name")).show()

df_last = df.select(last("name")).show()

df_max = df.select(max("name")).show()

df_min = df.select(min("name")).show()

df_sum = df.select(sum("name")).show()

spark.stop()

