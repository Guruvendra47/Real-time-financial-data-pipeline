# Docker Setup — Real-Time Financial Data Pipeline

## Architecture Overview

```text
Kafka → Spark → S3 → Snowflake → dbt → Power BI
                     ↑
                  Airflow
                     ↑
                  Docker
```

This pipeline runs completely on Docker for local development and testing.

---

# Step 1 — Install Docker

Download and install Docker from the official Docker website.

### Verify Installation

```bash
docker --version
```

### What Was Done

Docker was installed on the system.

### Purpose

Docker is used to run all required services as containers, including Kafka, Spark, Airflow, and Postgres.

---

# Step 2 — Navigate to Project Folder

```bash
cd real-time-financial-data-pipeline
```

### What Was Done

Moved into the project directory containing all configuration files.

### Purpose

Ensures Docker Compose can access the required files and services.

---

# Step 3 — Start All Services Using Docker Compose

Run the following command:

```bash
docker-compose up -d
```

### What Was Started

The following containers are launched:

* Zookeeper
* Kafka
* Postgres
* Spark Master
* Spark Worker
* Airflow Webserver
* Airflow Scheduler

### Purpose

Starts the complete real-time data pipeline locally using a single command.

---

# Step 4 — Verify Running Containers

```bash
docker ps
```

### Expected Containers

You should see containers similar to:

* kafka
* spark-master
* airflow
* postgres

### Purpose

Verifies that all required services are running successfully.

---

# Step 5 — Access Airflow UI

Open the following URL in your browser:

```text
http://localhost:8081
```

### Login Credentials

```text
Username: admin
Password: admin
```

### What Was Done

Accessed the Airflow web interface.

### Purpose

Airflow is used to orchestrate and manage the pipeline workflow, including Spark and dbt jobs.

---

# Step 6 — Create Kafka Topic

Run the topic creation script:

```bash
bash create_topic.sh
```

### What Was Done

Created the Kafka topic required for streaming data.

### Purpose

Kafka topics are required for producers and consumers to exchange streaming data.

---

# Step 7 — Run Kafka Producer

```bash
python kafka-producer.py
```

### Important Docker Configuration

Ensure the Kafka broker is configured as:

```python
"kafka:29092"
```

This configuration allows containers to communicate correctly within the Docker network.

### What Was Done

Started streaming real-time financial data into Kafka.

### Purpose

Simulates real-time data ingestion into the pipeline.

---

# Step 8 — Run Spark Streaming Through Airflow

The Airflow DAG automatically handles Spark job execution.

### In Airflow

1. Enable the DAG
2. Trigger the DAG manually

### Internal Spark Execution

```bash
spark-submit --master spark://spark-master:7077
```

### What Was Done

Triggered the Spark streaming job using Airflow.

### Purpose

Processes streaming Kafka data and loads it into storage layers.

---

# Step 9 — Spark Streaming Processing

### Spark Job Responsibilities

* Reads streaming data from Kafka (`kafka:29092`)
* Converts JSON data into structured format
* Writes data into:

  * Raw Layer
  * Processed Layer
  * Curated Layer

### What Was Done

Processed streaming financial data using Spark.

### Purpose

Implements a modern data lake architecture using Bronze, Silver, and Gold layers.

---

# Step 10 — dbt Transformation (Optional)

### Process

* Airflow triggers dbt jobs
* dbt transforms data inside Snowflake

### What Was Done

Performed analytics transformations on processed data.

### Purpose

Prepares business-ready and analytics-ready datasets.

---

# Step 11 — Stop All Services

```bash
docker-compose down
```

### What Was Done

Stopped and removed all running containers.

### Purpose

Safely shuts down the local pipeline environment.

---

# Execution Flow

```text
docker-compose up
        ↓
Create Kafka topic
        ↓
Run Kafka producer
        ↓
Trigger Airflow DAG
        ↓
Spark streaming job starts
        ↓
Data stored in S3
        ↓
dbt transformations executed
```

---

# Important Project Improvements

## Add `.dockerignore`

Create a `.dockerignore` file:

```bash
__pycache__/
*.log
dbt-env/
```

### Purpose

Prevents unnecessary files from being copied into Docker images, improving performance and reducing image size.

---

# Docker vs Kubernetes

| Docker                           | Kubernetes                     |
| -------------------------------- | ------------------------------ |
| Simple local setup               | Enterprise orchestration       |
| Best for development and testing | Best for production deployment |
| Uses docker-compose              | Uses kubectl and Helm          |

### Recommended Learning Path

Docker is typically used first for development and testing, followed by Kubernetes for scalable production deployment. This project follows the same industry-standard progression.

---

# Summary

This Docker-based setup enables a complete real-time financial data pipeline locally using:

* Kafka for streaming
* Spark for processing
* Airflow for orchestration
* S3 for storage
* Snowflake for warehousing
* dbt for transformations
* Power BI for analytics and visualization

The setup provides a production-style architecture while remaining simple enough for local development and learning.
