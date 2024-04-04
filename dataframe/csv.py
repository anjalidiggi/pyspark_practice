from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

spark = SparkSession.builder.appName("ReadCSVWithSchema").getOrCreate()

data = [("Anjali", 25), ("Akanksha", 30), ("Neha", 28), ("DJ", 27)]

df = spark.createDataFrame(data, schema=schema)

df.write.csv("csv_with_schema_output", header=True)

df_csv = spark.read.csv("csv_with_schema_output", header=True, schema=schema)

df_csv.show()
