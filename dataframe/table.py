from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("Example").getOrCreate()

data = [("Anjali",), ("Abhishek",), ("Meera",), ("Govind",)]

df = spark.createDataFrame(data, ["name"])

managed_table_path = "path_to_managed_table"
external_table_path = "path_to_external_table"

df.write.saveAsTable("managed_table")

df.write.option("path", external_table_path).saveAsTable("external_table")

spark.stop()
