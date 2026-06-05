# FinStream: Real-Time Transaction Processing & Fraud Detection Pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-00A1E4?logo=databricks)
![dbt](https://img.shields.io/badge/dbt-1.8+-FF694B?logo=dbt)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-0.18+-FF6B6B)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9+-017CEE?logo=apacheairflow)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![License](https://img.shields.io/badge/License-MIT-green.svg)

**FinStream** is a real-time financial transaction processing platform built using modern data engineering technologies.

The pipeline ingests synthetic transactions through Kafka, validates them using schema contracts, processes them with PySpark Structured Streaming, stores them in a Delta Lake Medallion Architecture, and produces fraud-detection analytics through dbt.

---

## Architecture Overview

```text
Producer (Faker + Avro)
        ↓
Apache Kafka + Schema Registry
        ↓
┌──────────────────────┐
│   Bronze Layer       │
│ (raw transactions)   │
│ Delta Lake           │
└──────────────────────┘
        ↓
┌──────────────────────┐
│   Silver Layer       │
│ (cleaned & validated)│
│ Delta Lake           │
└──────────────────────┘
        ↓
┌──────────────────────┐
│    Gold Layer        │
│ (analytics marts)    │
│ dbt + DuckDB         │
└──────────────────────┘
        ↓
Great Expectations | Apache Airflow | Prometheus | Grafana
```

### Tech Stack

| Category | Technologies |
| :--- | :--- |
| **Programming** | Python 3.11 |
| **Streaming** | Apache Kafka |
| **Schema Management** | Avro, Confluent Schema Registry |
| **Stream Processing** | PySpark Structured Streaming |
| **Storage** | Delta Lake |
| **Analytics Engineering** | dbt, DuckDB |
| **Data Quality** | Great Expectations |
| **Orchestration** | Apache Airflow |
| **Monitoring** | Prometheus, Grafana |
| **Containerization** | Docker Compose |

---

## Key Highlights

- Built a real-time transaction processing pipeline using **Kafka + PySpark Structured Streaming**.
- Implemented a **Medallion Architecture** (Bronze → Silver → Gold) using Delta Lake.
- Enforced schema contracts through **Avro serialization** and Schema Registry.
- Achieved exactly-once processing semantics through Structured Streaming checkpoints.
- Added data quality validation using **Great Expectations**.
- Created fraud velocity analytics using **dbt + DuckDB**.
- Orchestrated workflows with **Apache Airflow**.
- Added operational monitoring using **Prometheus and Grafana**.

## Skills Demonstrated

- Data Engineering & Streaming Systems
- Distributed Processing & Lakehouse Architecture
- Data Modeling & Data Quality Engineering
- Workflow Orchestration & Containerization
- Observability & Monitoring

---

## Scale & Load Characteristics (Local MVP)

| Metric | Value |
| :--- | :--- |
| **Producer Rate** | ~50–100 synthetic transactions/sec |
| **Micro-Batch Interval** | ~2–5 seconds |
| **Environment** | Single-machine deployment |
| **Memory Requirement** | ~8–12 GB Docker RAM allocation |

## Data Model – Gold Layer

The Gold layer produces fraud velocity metrics:

| Column | Description |
| :--- | :--- |
| `user_id` | Unique account identifier |
| `transaction_count_1h` | Number of transactions in the last 1-hour window |
| `total_amount_1h` | Total transaction amount in the last hour |
| `is_high_velocity_flag` | Boolean flag for high-risk velocity patterns |

---

## Quick Start & Verification

### 1. Clone and Start

```bash
git clone https://github.com/aditya388182/finstream_proj_1.git finstream_pipeline
cd finstream_pipeline

# Starts Kafka, Schema Registry, Spark, Airflow, and Grafana
./start.sh
```

> **Note:** The first run may take 2–3 minutes while Docker downloads images and initializes internal databases.

### 2. Verify Services are Healthy

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Ensure `kafka`, `schema-registry`, `airflow-webserver`, and your Spark containers show as `Up`.

### 3. Access the UIs

| Service | URL | Credentials (User/Pass) | Action Required |
| :--- | :--- | :--- | :--- |
| **Airflow** | http://localhost:8080 | `admin` / `admin` | Toggle `finstream_realtime_dag` to ON |
| **Grafana** | http://localhost:3000 | `admin` / `admin` | Navigate to Dashboards -> FinStream Health |
| **Data Docs** | Local File System | N/A | Open `gx/uncommitted/data_docs/index.html` in browser |

---

## Project Structure

```text
finstream_pipeline/
├── src/
│   ├── producer/           # Data generator & Avro schemas
│   ├── spark_jobs/         # Bronze & Silver processing
│   └── data_quality/       # Great Expectations
├── docker/                 # Docker Compose files (Kafka, Registry, Airflow, etc.)
├── gold_analytics/         # dbt project (DuckDB)
├── gx/                     # Great Expectations expectations & checkpoints
├── orchestration/          # Airflow DAGs
└── infrastructure/         # Terraform (optional)
```

---

## Key Engineering Decisions

| Decision | Rationale | Trade-off Accepted |
| :--- | :--- | :--- |
| **Avro + Schema Registry** | Enforces strict schema typing at the source and drastically reduces network payload size compared to JSON. | Adds infrastructure overhead and makes raw Kafka topics harder to read without a deserializer. |
| **Delta Lake (Bronze & Silver)** | Adds ACID transactions, schema evolution, time travel, and efficient upserts for streaming workloads. | Slightly higher storage overhead due to `_delta_log` directory. |
| **PySpark Structured Streaming** | Provides native exactly-once semantics, watermarks, and stateful processing through checkpoints. | Higher memory and CPU usage than lightweight consumers. |
| **dbt + DuckDB for Gold** | Enables fast local SQL transformations with strong modeling capabilities and no cloud infrastructure cost. | Not designed for high concurrency or very large-scale analytics. |
| **Great Expectations** | Supports declarative data contracts with automatic documentation and validation. | Requires upfront effort to define and maintain expectation suites. |
| **Dead Letter Queue (DLQ)** | Prevents malformed records from breaking the main pipeline. | Requires manual or separate monitoring to track DLQ volume and content. |

---

## Testing & Validation

Run Great Expectations data quality checks manually against the Gold layer:

```bash
python -m src.data_quality.run_checkpoint
```
> **Note:** Validation failures will also cause the Airflow DAG to fail, preventing bad data from reaching downstream consumers.

Run dbt models and tests manually:

```bash
cd gold_analytics
dbt run
dbt test
```

---

## Troubleshooting

| Issue | Likely Cause | Solution |
| :--- | :--- | :--- |
| **Container exits with Code 137** | Docker ran out of memory (OOM). | Open Docker Desktop settings and increase RAM allocation to at least 12GB. |
| **Port Conflicts (e.g., 8080/9092)** | Another local service is using the port. | Stop local Postgres, Tomcat, or other Kafka instances running outside of this Docker network. |
| **Spark Checkpoint Corruption** | Improper shutdown during a write. | Delete the `_checkpoints` directory and restart. Bronze will safely replay from the earliest offset. |

---

## Teardown

Stop all services and cleanly remove networks without destroying your data volumes:

```bash
docker compose -f docker/observability-compose.yml down
docker compose -f docker/airflow-compose.yml down
docker compose -f docker/docker-compose.yml down
```

---

## Future Improvements

- Move from a local containerized Kafka broker to a managed cloud service (e.g., Confluent Cloud or AWS MSK).
- Integrate Great Expectations validation as a blocking task directly inside the Airflow DAG.
- Add automated testing for Spark jobs and dbt models.
- Improve the local development experience for testing streaming jobs without requiring the full Docker stack.
- Implement active real-time alerting by routing flagged transactions to a dedicated Kafka sink and triggering Slack webhooks.
- Deploy a dedicated OLAP serving layer (e.g., Apache Spark Thrift Server or Trino) to expose the Gold tier for highly concurrent BI reporting.
