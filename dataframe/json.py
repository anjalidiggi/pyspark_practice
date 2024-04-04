from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

spark = SparkSession.builder.appName("ReadJSONWithSchema").getOrCreate()

data = [{"name": "Anjali", "age": 25}, {"name": "Akanksha", "age": 30}, {"name": "Neha", "age": 28}, {"name": "DJ", "age": 27}]

df = spark.createDataFrame(data, schema=schema)

df.write.json("json_with_schema_output")

df_json = spark.read.json("json_with_schema_output", schema=schema)

df_json.show()
