from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, max, min
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType
import shutil
import os

class SparkStreamManager:
    def __init__(self):
        self.spark = None
        self.schema = None
        self.stream = None
        self.queries = {}
    
    def start_stream_session(self):
        # Clean and create checkpoints directory
        shutil.rmtree("checkpoints/", ignore_errors=True)
        os.makedirs("checkpoints/", exist_ok=True)
        
        # Initialize Spark session
        self.spark = SparkSession.builder.appName("StockPriceStream").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
    
    def set_spark_schema(self):
        schema = StructType([
            StructField("unix_timestamp", LongType(), True),
            StructField("stockname", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("volume", DoubleType(), True)
        ])
        self.schema = schema
        
    def start_raw_stream_from_socket(self, host="localhost", port=3456):
        raw_stream = self.spark.readStream.format("socket") \
            .option("host", host) \
            .option("port", port) \
            .load()
        self.stream = raw_stream.select(from_json(col("value"), self.schema).alias("data")).select("data.*")
    
    def define_query(self, query_name, query_df):
        self.queries[query_name] = query_df
        
    def write_stream_data(self, query_name, output_mode="complete", format_type="memory", checkpoint_location=None):
        if checkpoint_location is None:
            checkpoint_location = f"checkpoints/{query_name}/"

        # Prevent duplicate queries with same name
        for q in self.spark.streams.active:
            if q.name == query_name:
                print(f"[INFO] Query '{query_name}' is already running.")
                return q  # Return the existing query

        query = self.queries[query_name].writeStream \
            .outputMode(output_mode) \
            .format(format_type) \
            .queryName(query_name) \
            .trigger(processingTime="1 seconds") \
            .option("checkpointLocation", checkpoint_location) \
            .start()

        return query
    
    def run_query(self, query):
        query.awaitTermination()

    def define_stock_aggregation_query(self, query_name="stock_meta_table"):
        aggregated_stream = self.stream.groupBy("stockname").agg(
            max("open").alias("max_open"),
            max("close").alias("max_close"),
            max("volume").alias("max_volume"),
            min("open").alias("min_open"),
            min("close").alias("min_close"),
            min("volume").alias("min_volume"),
            (max("close") - min("open")).alias("price_diff")
        ).orderBy(col("price_diff").desc())

        self.define_query(query_name, aggregated_stream)
        self.write_stream_data(query_name)  # Don't block with awaitTermination here

    def read_stream_table(self, table_name):
        return self.spark.sql(f"SELECT * FROM {table_name}")

    def print_active_queries(self):
        print("[ACTIVE STREAMS]:")
        for q in self.spark.streams.active:
            print(f" - {q.name}, isActive: {q.isActive}, lastProgress: {q.lastProgress}")
