from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count

spark = SparkSession.builder.appName('Spark').getOrCreate()

simpleData = [
    ("Anjali", "Sales", 10000),
    ("Abhishek", "Sales", 2600),
    ("Amit", "Sales", 41100),
    ("Mahima", "Finance", 32000),
    ("Gayathri", "Sales", 32000),
    ("Akanksha", "Finance", 3300),
    ("DJ", "Finance", 3900),
    ("Neha", "Marketing", 3000),
    ("Kunj", "Marketing", 23000),
    ("Meera", "Sales", 6700)
]


df = spark.createDataFrame(simpleData, ["name", "department", "salary"])

agg_df = df.groupBy("department").agg(
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary"),
    min("salary").alias("min_salary"),
    count("name").alias("total_count")
)


agg_df.show(truncate=False)


spark.stop()

