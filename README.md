# Real-Time Financial Data Pipeline

A real-time financial data pipeline built using modern data engineering technologies.

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
Real-time-financial-data-pipeline/
│
├── Docker/
│   └── Readme.md
│
├── Kubernetes/
│   ├── Readme.md
│   └── kubernetes-cleanup-guide.md
│
└── README.md
```

## Author

**Guruvendra Singh**
