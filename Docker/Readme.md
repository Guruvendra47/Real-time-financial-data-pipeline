# Docker Setup — Real-Time Financial Data Pipeline

This setup runs the real-time financial data pipeline locally using Docker and Docker Compose.

## Architecture

```text
Kafka → Spark → S3 → Snowflake → dbt → Power BI
                     ↑
                  Airflow
                     ↑
                  Docker
```

## Prerequisites

* Docker
* Docker Compose
* Python
* Git

---

## 1. Install Docker

Download and install Docker from the official Docker website.

Verify the installation:

```bash
docker --version
```

---

## 2. Navigate to the Project

```bash
cd real-time-financial-data-pipeline
```

---

## 3. Start the Services

Start all services using Docker Compose:

```bash
docker-compose up -d
```

The following services are started:

* Zookeeper
* Kafka
* Postgres
* Spark Master
* Spark Worker
* Airflow Webserver
* Airflow Scheduler

---

## 4. Verify Running Containers

```bash
docker ps
```

Verify that the required containers are running, including:

```text
kafka
spark-master
airflow
postgres
```

---

## 5. Access Airflow

Open:

```text
http://localhost:8081
```

Login:

```text
Username: admin
Password: admin
```

Airflow is used to orchestrate the pipeline and manage Spark and dbt jobs.

---

## 6. Create the Kafka Topic

Run:

```bash
bash create_topic.sh
```

This creates the Kafka topic used for streaming financial data.

---

## 7. Start the Kafka Producer

```bash
python kafka-producer.py
```

For Docker, the Kafka broker should be configured as:

```text
kafka:29092
```

This allows services running inside the Docker network to communicate with Kafka.

---

## 8. Run Spark Streaming

The Spark job is triggered through the Airflow DAG.

In Airflow:

1. Enable the DAG.
2. Trigger the DAG.

Spark runs using:

```bash
spark-submit --master spark://spark-master:7077
```

The Spark job reads streaming data from Kafka and processes it for storage.

---

## 9. Spark Processing

The Spark streaming job:

* Reads financial data from Kafka
* Processes JSON data
* Converts the data into a structured format
* Writes data to the storage layers

```text
Raw Layer
    ↓
Processed Layer
    ↓
Curated Layer
```

---

## 10. dbt Transformation

Airflow can trigger the dbt transformation jobs.

dbt transforms the data inside Snowflake and prepares it for analytics.

```text
Processed Data
      ↓
   Snowflake
      ↓
     dbt
      ↓
Analytics-Ready Data
```

---

## 11. Stop the Services

Stop and remove the running containers:

```bash
docker-compose down
```

---

## Execution Flow

```text
Docker Compose
      ↓
Kafka
      ↓
Kafka Producer
      ↓
Airflow
      ↓
Spark Streaming
      ↓
S3
      ↓
Snowflake
      ↓
dbt
      ↓
Power BI
```

---

## Docker Configuration

Add a `.dockerignore` file to exclude unnecessary files:

```text
__pycache__/
*.log
dbt-env/
```

This keeps Docker builds smaller and prevents unnecessary files from being copied into images.

---

## Docker and Kubernetes

| Docker                        | Kubernetes                                 |
| ----------------------------- | ------------------------------------------ |
| Local development and testing | Container orchestration                    |
| Uses Docker Compose           | Uses kubectl and Helm                      |
| Simple local setup            | Scalable deployment                        |
| Suitable for development      | Suitable for production-style environments |

---

## Related Documentation

For the Kubernetes deployment, see:

[Kubernetes Setup](../Kubernetes/Readme.md)

## Summary

This Docker setup provides a complete local environment for the real-time financial data pipeline using:

* Kafka for streaming
* Spark for processing
* Airflow for orchestration
* S3 for storage
* Snowflake for warehousing
* dbt for transformations
* Power BI for analytics and visualization

## Author

**Guruvendra Singh**
