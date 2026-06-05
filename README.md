# FinStream: Real-Time Transaction Processing & Fraud Detection Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-00A1E4?logo=databricks)
![dbt](https://img.shields.io/badge/dbt-1.8+-FF694B?logo=dbt)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-0.18+-FF6B6B)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9+-017CEE?logo=apacheairflow)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)

**FinStream** is a real-time data pipeline that ingests synthetic financial transactions and processes them through a strict **Medallion Architecture (Bronze → Silver → Gold)**.

It uses Delta Lake, PySpark Structured Streaming, dbt, and Great Expectations to demonstrate streaming, lakehouse, analytics engineering, and data quality patterns in a local environment.

---

# What This Project Demonstrates

- Streaming data ingestion with **exactly-once semantics** using PySpark Structured Streaming and Delta Lake checkpoints.
- **Strict Data Contracts** using Avro serialization and a containerized Confluent Schema Registry.
- **Lakehouse implementation** with ACID transactions, schema evolution, and time travel on Bronze and Silver layers.
- Data quality enforcement using **Great Expectations** as executable contracts on the Gold layer.
- Analytical transformations for fraud velocity metrics built with **dbt + DuckDB**.
- Observable pipeline using Airflow, Prometheus, and Grafana.

---

# Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Minimum 16 GB RAM recommended
- macOS or Linux (tested on MacBook Air M2)

---

# Data Model – Gold Layer

The Gold layer produces fraud velocity metrics.

| Column | Description |
|----------|------------|
| `user_id` | Unique account identifier |
| `transaction_count_1h` | Number of transactions in the last 1-hour window |
| `total_amount_1h` | Total transaction amount in the last hour |
| `is_high_velocity_flag` | Boolean flag for high-risk velocity patterns |

---

# Architecture Overview

```text
Producer (Faker + Avro)
        ↓
Apache Kafka + Schema Registry
        ↓
┌──────────────────────┐
│   Bronze Layer       │ ← PySpark Structured Streaming + Delta Lake
│   (raw transactions) │    (immutable, checkpointed)
└──────────────────────┘
        ↓
┌──────────────────────┐
│   Silver Layer       │ ← PySpark + Delta Lake
│   (cleaned & validated) │ (schema enforcement, deduplication, DLQ)
└──────────────────────┘
        ↓
┌──────────────────────┐
│   Gold Layer         │ ← dbt transformations
│   (analytics marts)  │    (fraud_velocity in DuckDB)
└──────────────────────┘
        ↓
Great Expectations + Airflow + Grafana Monitoring
```

The diagram above shows the complete data flow:

1. Synthetic transactions are generated using Faker.
2. Events are serialized using Avro and validated through Schema Registry.
3. Records are published to Kafka.
4. PySpark Structured Streaming ingests events into the Bronze Delta Lake layer.
5. Silver performs validation, deduplication, and DLQ routing.
6. dbt transforms Silver data into fraud analytics models in Gold.
7. Great Expectations validates business rules before downstream consumption.

---

# Quick Start

Clone the repository:

```bash
git clone https://github.com/aditya388182/finstream_proj_1.git finstream_pipeline

cd finstream_pipeline

./start.sh
```

> **Note:** The first run may take 2–3 minutes while Docker downloads images and starts all services.

---

# Accessing Services

## Airflow

```text
http://localhost:8080
```

Unpause:

```text
finstream_realtime_dag
```

## Grafana

```text
http://localhost:3000
```

## Great Expectations Data Docs

```text
gx/uncommitted/data_docs/
```

---

# Project Structure

```text
finstream_pipeline/
├── src/
│   ├── producer/           # Data generator & Avro schemas
│   ├── spark_jobs/         # Bronze & Silver processing
│   └── data_quality/       # Great Expectations
│
├── docker/                 # Docker Compose files
│                            # Kafka, Schema Registry,
│                            # Airflow, Grafana, etc.
│
├── gold_analytics/         # dbt project (DuckDB)
├── gx/                     # Great Expectations expectations & checkpoints
├── orchestration/          # Airflow DAGs
└── infrastructure/         # Terraform (optional)
```

---

# Key Engineering Decisions

| Decision | Rationale | Trade-off Accepted |
|-----------|------------|-------------------|
| Avro + Schema Registry | Enforces strict schema typing at the source and reduces network payload size compared to JSON | Additional infrastructure and more difficult topic inspection |
| Delta Lake (Bronze & Silver) | Provides ACID transactions, schema evolution, time travel, and efficient streaming writes | Increased storage overhead from `_delta_log` metadata |
| PySpark Structured Streaming | Exactly-once semantics, checkpoint recovery, watermarks, and stateful processing | Higher memory and CPU consumption |
| dbt + DuckDB | Fast local analytics with maintainable SQL transformations and zero cloud cost | Single-node execution model |
| Great Expectations | Declarative data contracts with validation and documentation generation | Ongoing expectation suite maintenance |
| Dead Letter Queue (DLQ) | Isolates malformed records and protects the primary pipeline | Requires separate monitoring and investigation |

---

# Data Quality & Validation

Great Expectations validates critical business rules on the Gold layer.

Run validations manually:

```bash
python -m src.data_quality.run_checkpoint
```

Validation failures also cause the Airflow DAG to fail, preventing bad data from reaching downstream consumers.

---

# Observability

The platform includes monitoring and operational visibility through:

## Prometheus

Tracks:

- Pipeline health
- Service availability
- Processing metrics

## Grafana

Visualizes:

- Processing lag
- Data freshness
- Pipeline status

## Airflow

Provides:

- Workflow orchestration
- Retry management
- Failure visibility

## Delta Lake Checkpointing

Provides:

- Fault tolerance
- Recovery after restarts
- Exactly-once guarantees

---

# Development Environment

This project was developed and tested on:

- MacBook Air M2
- 16 GB RAM
- Docker Desktop
- macOS

---

# Teardown

Stop all services:

```bash
docker compose -f docker/observability-compose.yml down

docker compose -f docker/airflow-compose.yml down

docker compose -f docker/docker-compose.yml down
```

---

# Future Improvements

## Infrastructure

- Move from a local Kafka broker to a managed cloud service such as Confluent Cloud or AWS MSK.
- Add multi-broker Kafka support.

## Testing

- Spark unit tests
- dbt model tests
- End-to-end integration tests

## Developer Experience

- Faster local testing for streaming jobs.
- Reduced dependency on the full Docker stack.

## Real-Time Alerting

- Route high-risk fraud events into a dedicated Kafka topic.
- Trigger Slack or PagerDuty notifications.

## Analytics Serving

- Apache Spark Thrift Server
- Trino
- Dedicated OLAP serving layer for BI workloads.
