from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, month, year, substring, lpad, when, expr, regexp_extract, dayofmonth, \
    concat_ws, to_timestamp

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
            .withColumn("day", dayofmonth("issue_date")) \
            .withColumn("month", month("issue_date")) \
            .withColumn("year", year("issue_date"))
df_time = df_time.withColumn("date_str", concat_ws("-", col("year"), col("month"), col("day")))
df_time = df_time.withColumn("timestamp", to_timestamp(col("date_str"), "yyyy-M-d"))
df_time = df_time.withColumn("valid_time", regexp_extract(col("Violation Time"), r"^(\d{4})([APap])$", 0))
# invalid time --> None
df_time = df_time.withColumn("raw_time", when(col("valid_time") != "", substring(col("Violation Time"), 1, 4)).otherwise(None))
df_time = df_time.withColumn("meridiem", when(col("valid_time") != "", substring(col("Violation Time"), 5, 1)).otherwise(None))
# just to be safe, pad with 0
df_time = df_time.withColumn("raw_time", lpad("raw_time", 4, "0"))
df_time = df_time.withColumn("hour", when(col("raw_time").isNotNull(), substring("raw_time", 1, 2).cast("int")))
df_time = df_time.withColumn("minute", when(col("raw_time").isNotNull(), substring("raw_time", 3, 2).cast("int")))
# invalid time --> -1
df_time = df_time.withColumn("hour_24",
    when(col("hour").isNull(), -1)
    .when((col("meridiem") == "P") & (col("hour") < 12), col("hour") + 12)
    .when((col("meridiem") == "A") & (col("hour") == 12), 0)
    .otherwise(col("hour"))
)
df_time = df_time.withColumn(
    "hour_24",
    when((col("hour_24") > 23) | (col("hour_24") < 0), -1).otherwise(col("hour_24"))
)
df_time = df_time.withColumn("violation_time_24",
    when(col("hour_24") == -1, "Invalid")
    .otherwise(expr("lpad(hour_24, 2, '0') || ':' || lpad(minute, 2, '0')"))
)

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

hotspots_hourly = df_time.groupBy("hour_24").count().orderBy("hour_24", ascending=True)
hotspots_hourly.show(10)

hotspots_hourly.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "http://elasticsearch:9200")\
    .option("es.nodes.discovery", "false")\
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .mode("overwrite")\
    .save("hotspots_hourly")

hotspots_timeline = df_time.groupBy("timestamp").count().orderBy("timestamp")
hotspots_timeline.show(10)

hotspots_timeline.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "http://elasticsearch:9200")\
    .option("es.nodes.discovery", "false")\
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .mode("overwrite")\
    .save("hotspots_timeline")

spark.stop()
