"""PySpark Data Processing Pipeline"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("DataPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

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

print("Reading from Kafka...")
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "raw-events") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("revenue", col("price") * col("quantity")) \
 .withColumn("event_date", to_date(to_timestamp(col("timestamp"))))

print("Creating aggregations...")
metrics = parsed.groupBy("user_id", "event_date").agg(
    count("*").alias("event_count"),
    sum("revenue").alias("total_revenue")
)

query = metrics.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="30 seconds") \
    .start()

print("✓ Streaming started! View at http://localhost:4040")
query.awaitTermination()
