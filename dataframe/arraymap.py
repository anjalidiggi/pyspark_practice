from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, lit, create_map

spark = SparkSession.builder.appName("ArrayMapFunctions").getOrCreate()

data = [("Anjali", 21), ("Akanksha", 30), ("Neha", 28), ("DJ", 27)]
schema = ["name", "age"]

df = spark.createDataFrame(data, schema=schema)

df_array = df.withColumn("info_array", array(col("name"), col("age")))
df_array.show(truncate=False)

df_map = df.withColumn("info_map", create_map(lit("name"), col("name"), lit("age"), col("age")))
df_map.show(truncate=False)

df_array_map = df_array.withColumn("first_name", col("info_array")[0])\
                       .withColumn("info_map_age", col("info_map")["age"])
df_array_map.show(truncate=False)


df_explode = df_array.select(col("name"), col("age"), col("info_array"))\
                     .withColumn("info_exploded", col("info_array"))\
                     .select(col("name"), col("age"), col("info_exploded"))
df_explode.show(truncate=False)


df_map_explode = df_map.select(col("name"), col("age"), col("info_map"))\
                       .withColumn("info_exploded", explode(col("info_map")))
df_map_explode.show(truncate=False)
