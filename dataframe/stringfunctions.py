

from pyspark.sql import SparkSession
from pyspark.sql.functions import upper

spark = SparkSession.builder.appName("StringFunctionsExample").getOrCreate()

data = [("Anjali", 21), ("Akanksha", 22), ("DJ", 23)]
columns = ["name", "age"]

df = spark.createDataFrame(data, schema=columns)

df.withColumn("upper_name", upper(df["name"])).show()


from pyspark.sql.functions import concat, lit

df.withColumn("name_age", concat(df["name"], lit("_"), df["age"])).show()



from pyspark.sql.functions import substring

df.withColumn("name_substring", substring(df["name"], 1, 3)).show()



from pyspark.sql.functions import length

df.withColumn("name_length", length(df["name"])).show()


from pyspark.sql.functions import split

df.withColumn("name_split", split(df["name"], "")).show()



from pyspark.sql.functions import regexp_replace

df.withColumn("name_replace", regexp_replace(df["name"], "a", "x")).show()
