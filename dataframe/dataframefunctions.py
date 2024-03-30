
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

data = [("Anjali", 21), ("Akanksha", 22), ("DJ", 23)]
columns = ["name", "age"]

df = spark.createDataFrame(data, schema=columns)

df.show()


df.select("name").show()


df.filter(df["age"] > 21).show()


df.withColumn("age_plus_10", df["age"] + 10).show()


df.groupBy("name").agg({"age": "avg"}).show()


df.orderBy("age").show()


df2 = spark.createDataFrame([("Anjali", "Sales"), ("Akanksha", "Marketing")], ["name", "department"])
df.join(df2, "name").show()


data = [("Anjali", "Sales", 1000), ("Akanksha", "Marketing", 1500), ("Anjali", "Marketing", 2000)]
columns = ["name", "department", "salary"]
df = spark.createDataFrame(data, schema=columns)

df.groupBy("name").pivot("department").sum("salary").show()



from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def square(x):
    return x * x

square_udf = udf(square, IntegerType())
df.withColumn("age_squared", square_udf(df["age"])).show()
