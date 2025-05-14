from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, month, year

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
# df.printSchema()

##################################
## Location based hotspots
##################################

# Rename for ES/Kibana compatibility
df_cleaned = df.withColumnRenamed("Street Name", "street_name")
df_cleaned = df_cleaned.withColumnRenamed("Violation Location", "violation_location")
df_cleaned = df_cleaned.withColumnRenamed("Violation County", "violation_county")

hotspots_street = df_cleaned.groupBy("street_name").count().orderBy("count", ascending=False)
# hotspots_street = hotspots_street.filter(hotspots_street["count"] > 100)
hotspots_street.show(10)

hotspots_street.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "http://elasticsearch:9200")\
    .option("es.nodes.discovery", "false")\
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .mode("overwrite")\
    .save("hotspots_street")

hotspots_location = df_cleaned.groupBy("violation_location").count().orderBy("count", ascending=False)
# hotspots_location = hotspots_location.filter(hotspots_location["count"] > 100)
hotspots_location.show(10)

hotspots_location.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "http://elasticsearch:9200")\
    .option("es.nodes.discovery", "false")\
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .mode("overwrite")\
    .save("hotspots_location")

hotspots_county = df_cleaned.groupBy("violation_county").count().orderBy("count", ascending=False)
# hotspots_county = hotspots_county.filter(hotspots_county["count"] > 100)
hotspots_county.show(10)

hotspots_county.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "http://elasticsearch:9200")\
    .option("es.nodes.discovery", "false")\
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .mode("overwrite")\
    .save("hotspots_county")

####################################
## Time based hotspots
####################################

df_time = df.withColumn("issue_date", to_date(col("Issue Date"), "MM/dd/yyyy")) \
            .withColumn("month", month("issue_date")) \
            .withColumn("year", year("issue_date"))

hotspots_monthly = df_time.groupBy("year", "month").count().orderBy("year", "month")
# hotspots_monthly = hotspots_monthly.filter(hotspots_monthly["count"] > 100)
hotspots_monthly.show(10)

hotspots_monthly.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "http://elasticsearch:9200")\
    .option("es.nodes.discovery", "false")\
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .mode("overwrite")\
    .save("hotspots_monthly")

spark.stop()
