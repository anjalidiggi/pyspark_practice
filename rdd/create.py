from pyspark import SparkContext

sc = SparkContext("local", "RDDExample")

names = ["Anjali", "Akanksha", "DJ"]
rdd = sc.parallelize(names)

print(rdd.collect())

sc.stop()
