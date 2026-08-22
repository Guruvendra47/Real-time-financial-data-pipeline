# ==========================================
# IMPORT LIBRARIES
# ==========================================
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, window, avg, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# ==========================================
# READ CONFIG FROM ENV
# WHY:
# Keeping credentials out of code is safer and makes the job portable
# across Docker, local, and future cluster deployments.
# ==========================================
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
bucket_name = os.getenv("S3_BUCKET", "real-time-financial-data-pipeline")

if not aws_access_key or not aws_secret_key:
    raise ValueError("Missing AWS credentials in environment variables")

# ==========================================
# CREATE SPARK SESSION
# WHY:
# spark-submit should control the cluster master.
# The app should stay portable and not hardcode the master URL.
# ==========================================
spark = SparkSession.builder \
    .appName("Kafka_S3_Streaming_Project") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==========================================
# HADOOP / S3 CONFIGURATION
# WHY:
# Spark needs S3A credentials and region access to write to S3.
# ==========================================
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", aws_access_key)
hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
hadoop_conf.set("fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

# ==========================================
# DEFINE SCHEMA
# WHY:
# Kafka messages are JSON strings, so Spark needs a schema to parse them safely.
# ==========================================
schema = StructType([
    StructField("p", DoubleType()),   # price
    StructField("s", StringType()),   # symbol
    StructField("t", LongType()),     # timestamp in milliseconds
    StructField("v", DoubleType())    # volume
])

# ==========================================
# READ FROM KAFKA
# WHY:
# This is the live streaming input topic.
# ==========================================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "trades") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# ==========================================
# CAST KAFKA VALUE TO STRING
# WHY:
# Kafka values are bytes. We must convert them to string before JSON parsing.
# ==========================================
string_df = kafka_df.selectExpr("CAST(value AS STRING) AS value")

# ==========================================
# PARSE JSON INTO STRUCTURED COLUMNS
# WHY:
# This turns raw JSON into typed columns for downstream writing and analytics.
# ==========================================
json_df = string_df.select(
    from_json(col("value"), schema).alias("data")
)

final_df = json_df.select("data.*")

clean_df = final_df.select(
    col("p").alias("price"),
    col("s").alias("symbol"),
    to_timestamp(from_unixtime(col("t") / 1000)).alias("timestamp"),
    col("v").alias("volume")
)

# ==========================================
# OPTIONAL DEBUG STREAM
# WHY:
# Useful while validating the pipeline. Remove later if you want fewer sinks.
# ==========================================
console_query = clean_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# ==========================================
# BRONZE LAYER (RAW / ODS)
# WHY:
# Raw immutable file landing zone in S3.
# This is your source of truth and replay layer.
# ==========================================
s3_raw_path = f"s3a://{bucket_name}/raw/trades/"
raw_checkpoint_path = f"s3a://{bucket_name}/checkpoint/raw/"

s3_raw_query = clean_df.writeStream \
    .format("parquet") \
    .option("path", s3_raw_path) \
    .option("checkpointLocation", raw_checkpoint_path) \
    .outputMode("append") \
    .start()

# ==========================================
# SILVER LAYER (CLEANED / VALIDATED)
# WHY:
# This keeps only valid rows and prepares them for analytics.
# ==========================================
processed_df = clean_df.filter(
    col("price").isNotNull() &
    col("symbol").isNotNull() &
    col("timestamp").isNotNull() &
    col("volume").isNotNull()
)

processed_path = f"s3a://{bucket_name}/processed/trades/"
processed_checkpoint = f"s3a://{bucket_name}/checkpoint/processed/"

processed_query = processed_df.writeStream \
    .format("parquet") \
    .option("path", processed_path) \
    .option("checkpointLocation", processed_checkpoint) \
    .outputMode("append") \
    .start()

# ==========================================
# GOLD LAYER (BUSINESS / ANALYTICS)
# WHY:
# Aggregated metrics for reporting and BI.
# ==========================================
watermarked_df = processed_df.withWatermark("timestamp", "1 minute")

curated_df = watermarked_df.groupBy(
    window(col("timestamp"), "1 minute"),
    col("symbol")
).agg(
    avg("price").alias("avg_price"),
    sum("volume").alias("total_volume")
)

curated_path = f"s3a://{bucket_name}/curated/trades/"
curated_checkpoint = f"s3a://{bucket_name}/checkpoint/curated/"

curated_query = curated_df.writeStream \
    .format("parquet") \
    .option("path", curated_path) \
    .option("checkpointLocation", curated_checkpoint) \
    .outputMode("append") \
    .start()

# ==========================================
# KEEP STREAM RUNNING
# WHY:
# Structured Streaming is a long-running job.
# ==========================================
spark.streams.awaitAnyTermination()


"""
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
--conf spark.executor.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
/opt/project/spark_processing/spark-stream-s3-aws.py
"""