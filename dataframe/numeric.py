from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, min, max, round, abs

spark = SparkSession.builder.appName("SparkFunctions").getOrCreate()

data = [("Anjali", 10), ("Gayathri", 20), ("Akanksha", -30), ("DJ", 40), ("ABCD", -50)]
df = spark.createDataFrame(data, ["name", "value"])

sum_df = df.select(sum(col("value")).alias("sum_value"))

avg_df = df.select(avg(col("value")).alias("avg_value"))

min_df = df.select(min(col("value")).alias("min_value"))

max_df = df.select(max(col("value")).alias("max_value"))

round_df = df.select(col("name"), round(col("value"), 2).alias("rounded_value"))

abs_df = df.select(col("name"), abs(col("value")).alias("abs_value"))

sum_df.show()
avg_df.show()
min_df.show()
max_df.show()
round_df.show()
abs_df.show()

spark.stop()
