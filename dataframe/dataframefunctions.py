#creatig a dataframe
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

data = [("Anjali", 21), ("Akanksha", 22), ("DJ", 23)]
columns = ["name", "age"]

df = spark.createDataFrame(data, schema=columns)

df.show()
#Selecting Columns:\


df.select("name").show()
#Filtering Data

df.filter(df["age"] > 21).show()
#Adding a New Column


df.withColumn("age_plus_10", df["age"] + 10).show()
#Grouping and Aggregating


df.groupBy("name").agg({"age": "avg"}).show()
#Sorting Data


df.orderBy("age").show()
#Joining DataFrames


df2 = spark.createDataFrame([("Anjali", "Sales"), ("Akanksha", "Marketing")], ["name", "department"])
df.join(df2, "name").show()
#Pivoting Data


data = [("Anjali", "Sales", 1000), ("Akanksha", "Marketing", 1500), ("Anjali", "Marketing", 2000)]
columns = ["name", "department", "salary"]
df = spark.createDataFrame(data, schema=columns)

df.groupBy("name").pivot("department").sum("salary").show()
#User-defined Functions (UDFs)


from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def square(x):
    return x * x

square_udf = udf(square, IntegerType())
df.withColumn("age_squared", square_udf(df["age"])).show()
