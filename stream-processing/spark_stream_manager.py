from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, max, min
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
import shutil
import os

# Remove and recreate 'checkpoints' directory
shutil.rmtree("checkpoints/", ignore_errors=True)
os.makedirs("checkpoints/", exist_ok=True)

# Initialize Spark Session
spark = SparkSession.builder.appName("StockPriceStream").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # Reduce logs

# Define schema for incoming JSON data
schema = StructType([
    StructField("unix_timestamp", LongType(), True),
    StructField("stockname", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

# Read stream from socket
raw_stream = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 3456) \
    .load()

# Parse JSON data
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Aggregate stock data
aggregated_stream = parsed_stream.groupBy("stockname").agg(
    max("open").alias("max_open"),
    max("close").alias("max_close"),
    max("volume").alias("max_volume"),
    min("open").alias("min_open"),
    min("close").alias("min_close"),
    min("volume").alias("min_volume"),  
    (max("close") - min("open")).alias("price_diff")
)

# Sorting (applied only once)
sorted_stream = aggregated_stream.orderBy(col("price_diff").desc())

# Start the query
# query = sorted_stream.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .queryName("stock_analysis") \
#     .trigger(processingTime="1 second") \
#     .option("checkpointLocation", "checkpoints/stock_analysis/") \
#     .start()

# track all changes on parameters upto now .
query_1 = sorted_stream.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("stock_meta_table") \
    .trigger(processingTime="5 seconds") \
    .start()
    
# Wait for query termination
query_1.awaitTermination()
