from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Parking Violations") \
    .master("spark://spark:7077") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-hadoop:9.0.0") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:9.0.0") \
    .getOrCreate()

basedir = "/opt/bitnami/spark-app"

sc = spark.sparkContext
path = f"{basedir}/data/Parking_Violations_Issued_Fiscal_Year_2014.csv"

df = spark.read.csv(path)
print("------------------------")
print(df.rdd.getNumPartitions())
print("------------------------")
# df.show(5)

# df.write.format("org.elasticsearch.spark.sql")\
#     .option("es.nodes", "http://elasticsearch:9200")\
#     .option("es.nodes.discovery", "false")\
#     .option("es.nodes.wan.only", "true")\
#     .option("es.index.auto.create", "true")\
#     .mode("overwrite")\
#     .save("test_index")

spark.stop()