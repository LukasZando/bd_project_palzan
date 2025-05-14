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

df = spark.read.csv(path, header=True, inferSchema=True)
df.cache()
df.printSchema()

# Rename for ES/Kibana compatibility
df_cleaned = df.withColumnRenamed("Street Name", "street_name")

hotspots_street = df_cleaned.groupBy("street_name").count().orderBy("count", ascending=False)
hotspots_street.show(10)

hotspots_street.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "http://elasticsearch:9200")\
    .option("es.nodes.discovery", "false")\
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .mode("overwrite")\
    .save("hotspots_street")

spark.stop()