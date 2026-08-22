# ==========================================
# import libraries
# ==========================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


# ==========================================
# create spark session
# ==========================================
spark = SparkSession.builder \
    .appName("Kafka_Streaming_Project") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# ==========================================
# define data structure - (COMING FROM KAFKA)
# ==========================================
schema = StructType([
    StructField("p", DoubleType()),   # price
    StructField("s", StringType()),   # symbol
    StructField("t", LongType()),     # timestamp
    StructField("v", DoubleType())    # volume
])


# ==========================================
# read data from kafka
# ==========================================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "trades") \
    .option("startingOffsets", "latest") \
    .load()


# ==========================================
# convert binary → STRING
# ==========================================
string_df = kafka_df.selectExpr("CAST(value AS STRING)")


# ==========================================
# parse json data
# ==========================================
json_df = string_df.select(
    from_json(col("value"), schema).alias("data")
)


# ==========================================
# flatten data
# ==========================================
final_df = json_df.select("data.*")


# ==========================================
# rename columns
# ==========================================
clean_df = final_df.select(
    col("p").alias("price"),
    col("s").alias("symbol"),
    col("t").alias("timestamp"),
    col("v").alias("volume")
)


# ==========================================
# print to CONSOLE (DEBUG)
# ==========================================
console_query = clean_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()


# ==========================================
# write to SNOWFLAKE add username and password
# ==========================================
def write_to_snowflake(batch_df, batch_id):
    batch_df.write \
        .format("snowflake") \
        .option("sfURL", "your_account.snowflakecomputing.com") \
        .option("sfUser", "YOUR_USERNAME") \
        .option("sfPassword", "YOUR_PASSWORD") \
        .option("sfDatabase", "YOUR_DB") \
        .option("sfSchema", "PUBLIC") \
        .option("sfWarehouse", "COMPUTE_WH") \
        .option("dbtable", "TRADES") \
        .mode("append") \
        .save()


snowflake_query = clean_df.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("append") \
    .start()


# ==========================================
# keep running
# ==========================================
spark.streams.awaitAnyTermination()