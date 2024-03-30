from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, upper, substring, length, concat


spark = SparkSession.builder \
    .appName("String Functions Example") \
    .getOrCreate()

data = [("Anjali",), ("Abhishek",), ("Meera",), ("Amit",)]

df = spark.createDataFrame(data, ["name"])

print("Original DataFrame:")
df.show()

df = df.withColumn("name_lower", lower(col("name")))  
df = df.withColumn("name_upper", upper(col("name")))  
df = df.withColumn("name_substr", substring(col("name"), 1, 3))  
df = df.withColumn("name_length", length(col("name")))  

print("DataFrame with String Functions:")
df.show()

spark.stop()
