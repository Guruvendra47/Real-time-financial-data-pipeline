````md
## Go inside the Docker container:

```bash
docker exec -it spark bash
````

## Executing Console file before database:

```bash
/opt/spark/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
--conf spark.executor.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
/opt/project/spark_processing/spark_stream_console.py
```

## Execute postgres(database) file after database:

```bash
/opt/spark/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
--conf spark.executor.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
/opt/project/spark_processing/spark-stream-postgres.py
```

```
```
## Execute aws s3(lakehouse) file after database:

```bash
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 \
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
--conf spark.executor.extraJavaOptions="-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
/opt/project/spark_processing/spark-stream-s3-aws.py
```