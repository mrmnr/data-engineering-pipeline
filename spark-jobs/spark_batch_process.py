"""PySpark Data Processing - Read from Kafka and Aggregate"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaDataPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming data
schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("session_id", StringType()),
    StructField("device", StringType()),
    StructField("location", StringType())
])

print("=" * 60)
print("Reading data from Kafka...")
print("=" * 60)

# Read from Kafka
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "raw-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON data
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Add calculated fields
enriched_df = parsed_df \
    .withColumn("revenue", col("price") * col("quantity")) \
    .withColumn("event_date", to_date(to_timestamp(col("timestamp"))))

print("\n📊 Sample Data:")
enriched_df.show(10, truncate=False)

print("\n📈 Aggregations - User Metrics:")
user_metrics = enriched_df.groupBy("user_id").agg(
    count("*").alias("total_events"),
    sum("revenue").alias("total_revenue"),
    countDistinct("session_id").alias("sessions"),
    collect_set("event_type").alias("event_types")
).orderBy(desc("total_revenue"))

user_metrics.show(10, truncate=False)

print("\n📊 Aggregations - Product Category Performance:")
category_metrics = enriched_df.groupBy("category").agg(
    count("*").alias("total_events"),
    sum("revenue").alias("total_revenue"),
    avg("price").alias("avg_price"),
    countDistinct("user_id").alias("unique_users")
).orderBy(desc("total_revenue"))

category_metrics.show()

print("\n📍 Aggregations - Location Analysis:")
location_metrics = enriched_df.groupBy("location").agg(
    count("*").alias("total_events"),
    sum("revenue").alias("total_revenue"),
    countDistinct("user_id").alias("unique_users")
).orderBy(desc("total_revenue"))

location_metrics.show()

print("\n✅ Data processing completed!")
print("=" * 60)

spark.stop()
