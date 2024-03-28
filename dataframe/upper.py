from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StringFunctions").getOrCreate()

data = [("Anjali",), ("Akanksha",), ("Gayathri",), ("DJ",)]
df = spark.createDataFrame(data, ["name"])

df = df.withColumn("upper", upper(col("name"))) 
df.show(truncate=False)

spark.stop()
