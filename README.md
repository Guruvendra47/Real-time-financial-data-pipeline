# Real-Time Financial Data Pipeline

A real-time financial data pipeline built using modern data engineering technologies.

## Architecture

```text
Kafka → Spark → S3 → Snowflake → dbt → Power BI
            ↑
         Airflow
```

## Technologies

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

[Docker Setup](./Docker/Readme.md)

### Kubernetes

Used for container orchestration and deployment.

[Kubernetes Setup](./Kubernetes/Readme.md)

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