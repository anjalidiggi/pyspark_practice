from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, date_add, datediff, year, month, day

spark = SparkSession.builder.appName("AgeCalculation").getOrCreate()

data = [("Ayush", "2016-12-13")]
columns = ["name", "dob"]
df = spark.createDataFrame(data, columns)

df = df.withColumn("current_date", current_date())
df = df.withColumn("age_years", year("current_date") - year("dob"))
df = df.withColumn("age_months", (year("current_date") - year("dob")) * 12 + (month("current_date") - month("dob")))
df = df.withColumn("age_days", datediff("current_date", "dob"))

df.show()

spark.stop()
