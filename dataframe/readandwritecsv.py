from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

df = spark.read.csv("path_to_csv_file.csv", header=True, schema=schema)

df.write.csv("path_to_output_csv_folder", mode="overwrite", header=True)

spark.stop()
