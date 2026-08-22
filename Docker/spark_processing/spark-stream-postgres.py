# ==========================================
# IMPORT LIBRARIES
# ==========================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


# ==========================================
# CREATE SPARK SESSION (CLUSTER + DEPENDENCIES)
# ==========================================
spark = SparkSession.builder \
    .appName("Kafka_Streaming_Project") \
    .master("spark://spark-master:7077") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.postgresql:postgresql:42.7.3"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# ==========================================
# DEFINE SCHEMA (KAFKA DATA)
# ==========================================
schema = StructType([
    StructField("p", DoubleType()),   # price
    StructField("s", StringType()),   # symbol
    StructField("t", LongType()),     # timestamp (epoch ms)
    StructField("v", DoubleType())    # volume
])


# ==========================================
# READ STREAM FROM KAFKA
# ==========================================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "trades") \
    .option("startingOffsets", "latest") \
    .load()


# ==========================================
# CONVERT BINARY → STRING
# ==========================================
string_df = kafka_df.selectExpr("CAST(value AS STRING)")


# ==========================================
# PARSE JSON DATA
# ==========================================
json_df = string_df.select(
    from_json(col("value"), schema).alias("data")
)


# ==========================================
# FLATTEN DATA
# ==========================================
final_df = json_df.select("data.*")


# ==========================================
# CLEAN + TRANSFORM DATA
# ==========================================
clean_df = final_df.select(
    col("p").alias("price"),
    col("s").alias("symbol"),
    from_unixtime(col("t") / 1000).alias("timestamp"),  # convert epoch → readable
    col("v").alias("volume")
)


# ==========================================
# WRITE TO CONSOLE (DEBUG)
# ==========================================
console_query = clean_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/console") \
    .start()


# ==========================================
# WRITE TO POSTGRES
# ==========================================
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/tradesdb") \
        .option("dbtable", "trades") \
        .option("user", "sparkuser") \
        .option("password", "sparkpass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


postgres_query = clean_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/postgres") \
    .start()


# ==========================================
# KEEP STREAM RUNNING
# ==========================================
spark.streams.awaitAnyTermination()