from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, arrays_zip
from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType, LongType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("StockPriceStream").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # Reduce logs

# Define schema for incoming JSON data
schema = StructType([
    StructField("unix_timestamp", ArrayType(LongType()), True),
    StructField("stockname", ArrayType(StringType()), True),
    StructField("open", ArrayType(DoubleType()), True),
    StructField("close", ArrayType(DoubleType()), True),
    StructField("high", ArrayType(DoubleType()), True),
    StructField("low", ArrayType(DoubleType()), True),
    StructField("volume", ArrayType(DoubleType()), True)
])

# Read stream from socket
raw_stream = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 3456) \
    .load()
    
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Zip all arrays together
zipped_stream = parsed_stream.withColumn("zipped", arrays_zip(
    col("unix_timestamp"),
    col("stockname"),
    col("open"),
    col("close"),
    col("high"),
    col("low"),
    col("volume")
))

# Explode into a structured DataFrame
flattened_stream = zipped_stream.select(
    explode(col("zipped")).alias("row")
).select(
    col("row.unix_timestamp").alias("unix_timestamp"),
    col("row.stockname").alias("stockname"),
    col("row.open").alias("open"),
    col("row.close").alias("close"),
    col("row.high").alias("high"),
    col("row.low").alias("low"),
    col("row.volume").alias("volume")
)

def get_stock_with_price(
    parameter: str,
    state: str,
    quantity: int = 1
):
    if state == "highest":
        return flattened_stream.orderBy(col(parameter).desc()).limit(quantity)
    else:
        return flattened_stream.orderBy(col(parameter).asc()).limit(quantity)

def get_difference(
    parameter1: str,
    parameter2: str,
    order:str,
    quantity: int = 1
):
    diff_df = flattened_stream.\
        withColumn(f"{parameter1}_{parameter2}_diff", 
        col(parameter1) - col(parameter2))
    if order == "highest":
        return diff_df.orderBy(col(f"{parameter1}_{parameter2}_diff").desc()).limit(quantity)
    else:
        return diff_df.orderBy(col(f"{parameter1}_{parameter2}_diff").asc()).limit(quantity)


# answer the questions 
# the stock with the highest opening price 
q1 = get_stock_with_price("open", "highest")
# the stock with the highest closing price
q2 = get_stock_with_price("close", "highest")
# the stock with the highest volume
q3 = get_stock_with_price("volume", "highest")
# the stock with the highest closing price - opening price difference
q4 = get_difference("close", "open", "highest")
# the stock with the lowest opening price 
q5 = get_stock_with_price("open", "lowest")
# the stock with the lowest closing price
q6 = get_stock_with_price("close", "lowest")
# the stock with the lowest volume
q7 = get_stock_with_price("volume", "lowest")
# the stock with the lowest closing price - opening price difference
q8 = get_difference("close", "open", "lowest")

# final query
queries_list = [q1, q2, q3, q4, q5, q6, q7, q8]

# Store running queries
running_queries = []

for i, q in enumerate(queries_list, start=1):
    query = q.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    running_queries.append(query)

# Await termination for all queries
for query in running_queries:
    query.awaitTermination()
