from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

data = [("Anjali",), ("Akanksha",), ("DJ",)]

schema = StructType([StructField("name", StringType(), True)])

df = spark.createDataFrame(data, schema)

df.show()

spark.stop()
