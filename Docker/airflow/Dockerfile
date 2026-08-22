FROM apache/airflow:2.9.3

USER root

# WHY: Spark needs Java, and we need curl/tar to download Spark.
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    curl \
    tar \
    && rm -rf /var/lib/apt/lists/*

# WHY: Add Spark client tools so Airflow can run spark-submit.
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

# WHY: Make dbt available in the Airflow container.
ENV PATH="/home/airflow/.local/bin:${PATH}"

USER airflow

# WHY: Install dbt as the airflow user, not root, and without --user.
RUN pip install --no-cache-dir dbt-snowflake

# for the first time you have to use --build terminal command
#d ocker-compose -f your-file-name.yaml down -v
# docker-compose -f your-file-name.yaml up -d --build