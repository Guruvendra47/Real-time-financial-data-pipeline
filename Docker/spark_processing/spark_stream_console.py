from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# ==========================================
# 1️CREATE SPARK SESSION
# ==========================================
spark = (
    SparkSession.builder
    .appName("Kafka_RealTime_Streaming")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ==========================================
# DEFINE INPUT SCHEMA (KAFKA JSON)
# ==========================================
trade_schema = StructType([
    StructField("p", DoubleType(), True),   # price
    StructField("s", StringType(), True),   # symbol
    StructField("t", LongType(), True),     # timestamp
    StructField("v", DoubleType(), True)    # volume
])

# ==========================================
# READ STREAM FROM KAFKA
# ==========================================
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "trades")
    .option("startingOffsets", "latest")
    .load()
)

# Convert Kafka binary value → string
kafka_string_df = kafka_df.selectExpr("CAST(value AS STRING)")

# ==========================================
# PARSE JSON DATA
# ==========================================
parsed_df = kafka_string_df.select(
    from_json(col("value"), trade_schema).alias("data")
)

# Flatten JSON structure
flattened_df = parsed_df.select("data.*")

# ==========================================
# CLEAN & RENAME COLUMNS
# ==========================================
clean_df = flattened_df.select(
    col("p").alias("price"),
    col("s").alias("symbol"),
    col("t").alias("timestamp"),
    col("v").alias("volume")
)

# ==========================================
# WRITE STREAM TO CONSOLE
# ==========================================
query = (
    clean_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

# Keep streaming alive
query.awaitTermination()