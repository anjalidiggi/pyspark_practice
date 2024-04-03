from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("UnionAll").getOrCreate()

data_anjali = [Row(id=1, name='Anjali'), Row(id=2, name='Anjali')]
data_abhishek = [Row(id=3, name='Abhishek'), Row(id=1, name='Abhishek')]
data_meera = [Row(id=4, name='Meera'), Row(id=5, name='Meera')]
data_akanksha = [Row(id=6, name='Akanksha'), Row(id=7, name='Akanksha')]
data_amit = [Row(id=8, name='Amit'), Row(id=9, name='Amit')]

anjali = spark.createDataFrame(data_anjali)
abhishek = spark.createDataFrame(data_abhishek)
meera = spark.createDataFrame(data_meera)
akanksha = spark.createDataFrame(data_akanksha)
amit = spark.createDataFrame(data_amit)

combined_all_df = meera.unionAll(akanksha)

print("Union All:")
combined_all_df.show()

spark.stop()
