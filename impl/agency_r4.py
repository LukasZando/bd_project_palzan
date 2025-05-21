from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, avg, min, max, stddev, countDistinct, date_format, udf, \
    to_utc_timestamp, to_timestamp, concat_ws, when
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("Agency Enforcement Strength Analysis") \
    .master("spark://spark:7077") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:9.0.0") \
    .getOrCreate()

basedir = "/opt/bitnami/spark-app"
path = f"{basedir}/data/Parking_Violations_Issued_Fiscal_Year_2014.csv"

df = spark.read.csv(path, header=True, inferSchema=True)
df.cache()

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
df = drop_duplicates_based_on_columns(df, relevant_columns)

df = df.withColumnRenamed("Issuing Agency", "issuing_agency")
df = df.withColumn("issue_date", to_date(col("issue_date"), "yyyy-MM-dd")) \
       .filter(col("issuing_agency").isNotNull()) \
       .filter(col("issue_date").isNotNull())

daily_counts = df.groupBy("issuing_agency", "issue_date").count()

agency_stats = daily_counts.groupBy("issuing_agency").agg(
    avg("count").alias("avg_tickets_per_day"),
    min("count").alias("min_tickets_per_day"),
    max("count").alias("max_tickets_per_day"),
    stddev("count").alias("stddev_tickets_per_day"),
    countDistinct("issue_date").alias("active_days")
)

agency_map = [
    ("A", "PORT AUTHORITY"),
    ("B", "TRIBOROUGH BRIDGE AND TUNNEL POLICE"),
    ("C", "CON RAIL"),
    ("D", "DEPARTMENT OF BUSINESS SERVICES"),
    ("E", "BOARD OF ESTIMATE"),
    ("F", "FIRE DEPARTMENT"),
    ("G", "TAXI AND LIMOUSINE COMMISSION"),
    ("H", "HOUSING AUTHORITY"),
    ("I", "STATEN ISLAND RAPID TRANSIT POLICE"),
    ("J", "AMTRAK RAILROAD POLICE"),
    ("K", "PARKS DEPARTMENT"),
    ("L", "LONG ISLAND RAILROAD"),
    ("M", "TRANSIT AUTHORITY"),
    ("N", "NYS PARKS POLICE"),
    ("O", "NYS COURT OFFICERS"),
    ("P", "POLICE DEPARTMENT"),
    ("Q", "DEPARTMENT OF CORRECTION"),
    ("R", "NYC TRANSIT AUTHORITY MANAGERS"),
    ("S", "DEPARTMENT OF SANITATION"),
    ("T", "TRAFFIC"),
    ("U", "PARKING CONTROL UNIT"),
    ("V", "DEPARTMENT OF TRANSPORTATION"),
    ("W", "HEALTH DEPARTMENT POLICE"),
    ("X", "OTHER/UNKNOWN AGENCIES"),
    ("Y", "HEALTH AND HOSPITAL CORP. POLICE"),
    ("Z", "METRO NORTH RAILROAD POLICE"),
    ("1", "NYS OFFICE OF MENTAL HEALTH POLICE"),
    ("2", "O M R D D"),
    ("3", "ROOSEVELT ISLAND SECURITY"),
    ("4", "SEA GATE ASSOCIATION POLICE"),
    ("5", "SNUG HARBOR CULTURAL CENTER RANGERS"),
    ("6", "WATERFRONT COMMISSION OF NY HARBOR"),
    ("7", "SUNY MARITIME COLLEGE"),
    ("9", "NYC OFFICE OF THE SHERIFF")
]

schema = StructType([
    StructField("issuing_agency", StringType(), True),
    StructField("agency_name", StringType(), True)
])

agency_df = spark.createDataFrame(agency_map, schema=schema)
agency_stats_named = agency_stats.join(agency_df, on="issuing_agency", how="left")

agency_stats_named.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "http://elasticsearch:9200")\
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .mode("overwrite")\
    .save("agency_daily_stats")

df_agency_county = df.withColumn("county_normalized",
    when(col("Violation County").isin("NY", "NYC", "MANHATTAN"), "MANHATTAN")
    .when(col("Violation County").isin("K", "KINGS"), "BROOKLYN")
    .when(col("Violation County").isin("Q", "QUEEN", "QUEENS"), "QUEENS")
    .when(col("Violation County").isin("BX", "BRONX"), "BRONX")
    .when(col("Violation County").isin("R", "RICH", "RC"), "RICHMOND")
    .otherwise("OTHER")
)

# Count violations grouped by agency and normalized county
violations_per_agency_per_county = df_agency_county \
    .groupBy("issuing_agency", "county_normalized") \
    .count() \
    .orderBy("count", ascending=False)

violations_agency_county_named = violations_per_agency_per_county.join(agency_df, on="issuing_agency", how="left")

violations_agency_county_named.show(20)

# Save to Elasticsearch
violations_agency_county_named.write.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "http://elasticsearch:9200")\
    .option("es.nodes.discovery", "false")\
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .mode("overwrite")\
    .save("violations_agency_county")

spark.stop()
