from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, date_format, to_utc_timestamp
from pyspark.sql.functions import udf, concat_ws, to_timestamp
from pyspark.sql.types import StringType


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

###
# Helper
###

def convert_violation_time(v_time):
    try:
        hour = int(v_time[:2])
        minute = int(v_time[2:4])
        am_pm = v_time[4].upper()

        if am_pm == 'P' and hour != 12:
            hour += 12
        elif am_pm == 'A' and hour == 12:
            hour = 0
        return f"{hour:02d}:{minute:02d}:00"

    except:
        return None

def drop_duplicates_based_on_columns(df, columns: list):
    before_count = df.count()

    df = df.dropDuplicates(columns)

    after_count = df.count()

    dropped_count = before_count - after_count
    print(f"Number of rows dropped: {dropped_count}")

    return df

df = df.withColumnRenamed("Plate ID", "plate_id")
df = df.withColumnRenamed("Street Name", "street_name")
df = df.withColumnRenamed("Violation Code", "violation_code")
df = df.withColumn("Issue Date", date_format(to_date(col("Issue Date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
df = df.withColumnRenamed("Issue Date", "issue_date")
df = df.withColumnRenamed("Violation Time", "violation_time")


####
# compute violation timestamp that is suitable for ES
#####

convert_violation_time_udf = udf(convert_violation_time, StringType())
df = df.withColumn("time_24hr", convert_violation_time_udf(col("violation_time")))

df = df.withColumn(
    "violation_iso_timestamp",
    date_format(to_utc_timestamp(to_timestamp(concat_ws(" ", col("issue_date"), col("time_24hr"))), "UTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
)

###
# Final df
###

relevant_columns = ["plate_id", "street_name", "violation_code", "violation_iso_timestamp"]
df = df.select(relevant_columns)
df = drop_duplicates_based_on_columns(df, relevant_columns)

df.write.format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "http://elasticsearch:9200") \
    .option("es.nodes.discovery", "false") \
    .option("es.nodes.wan.only", "true") \
    .option("es.index.auto.create", "true") \
    .mode("overwrite") \
    .save("time_total")

spark.stop()
