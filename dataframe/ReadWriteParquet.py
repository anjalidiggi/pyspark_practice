from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

df = spark.read.parquet("path_to_parquet_file.parquet")

df.write.parquet("path_to_output_parquet_folder", mode="overwrite")

spark.stop()
