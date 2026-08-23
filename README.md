# Real-Time Financial Data Pipeline

A real-time financial data pipeline built using modern data engineering technologies.

## Business Problem

Financial market data changes continuously and needs to be available quickly for analysis and decision-making. Processing market data manually or through batch workflows can introduce delays and make it difficult to monitor market activity in near real time.

A real-time data pipeline is required to continuously collect financial market data, process streaming data efficiently, store it in scalable storage and a data warehouse, and make it available for analytics and visualization.

## Business Objective

The objective of this project is to build a real-time financial data pipeline that can:

* Extract financial market data from Finnhub through its API
* Stream the data through Kafka
* Process streaming data using Spark Streaming
* Store data in Amazon S3
* Load and manage analytical data in Snowflake
* Transform data using dbt
* Orchestrate workflows using Airflow
* Provide analytics and visualization through Power BI

## Business Questions

The pipeline can support questions such as:

* What is the current market activity for selected financial instruments?
* How are prices changing over time?
* What are the historical price trends?
* How can streaming market data be processed and made available for analysis with minimal delay?
* How can real-time data be transformed into analytics-ready data?

## Data Source

Financial market data is extracted from [Finnhub](https://finnhub.io/) using its API through the Kafka Producer, published to a Kafka Topic, and processed using Spark Streaming.

## Architecture

```text
Finnhub
   ↓
Kafka → Spark → S3 → Snowflake → dbt → Power BI
            ↑
         Airflow
```

## Technologies

* Finnhub
* Kafka
* Apache Spark
* Amazon S3
* Snowflake
* dbt
* Apache Airflow
* Power BI
* Docker
* Kubernetes

## Deployment Methods

This project supports two deployment methods.

### Docker

Used for local development and testing.

[Docker Setup](https://github.com/Guruvendra47/Real-time-financial-data-pipeline/blob/main/Docker/Readme.md)

### Kubernetes

Used for container orchestration and deployment.

[Kubernetes Setup](https://github.com/Guruvendra47/Real-time-financial-data-pipeline/blob/main/Kubernetes/Readme.md)

## Project Structure

```text
Real-time-financial-data-pipeline-project/
│
├── architecture/
│   └── architecture.png
│
├── Docker/
│   ├── airflow/
│   ├── dbt/
│   ├── kafka/
│   ├── spark_processing/
│   ├── docker-compose.yaml
│   ├── env
│   ├── init.sql
│   └── Readme.md
│
├── Kubernetes/
│   ├── configs/
│   ├── manifests/
│   ├── .gitignore
│   ├── kubernetes-cleanup-guide.md
│   └── Readme.md
│
├── Rough-notes.txt
└── README.md
```

## Author

**Guruvendra Singh**
