# System Architecture & Design Decisions

FinStream is a real-time data pipeline that ingests synthetic financial transactions and processes them through a Medallion Architecture (Bronze → Silver → Gold). This document explains the data flow, component choices, and key engineering trade-offs made during its design.

---

## Data Flow (Medallion Architecture)

The pipeline follows a strict three-layer structure to maintain data immutability, enforce schema quality, and deliver analytics-ready data.

```text
Producer (Faker + Avro/JSON)
        ↓
Apache Kafka (Docker)
        ↓
┌──────────────────────┐
│   Bronze Layer       │ ← PySpark Structured Streaming + Delta Lake
│   (raw transactions) │    (immutable, checkpointed)
└──────────────────────┘
        ↓
┌──────────────────────┐
│   Silver Layer       │ ← PySpark + Delta Lake
│   (cleaned & validated)│   (schema enforcement, deduplication, DLQ)
└──────────────────────┘
        ↓
┌──────────────────────┐
│   Gold Layer         │ ← dbt transformations
│   (analytics marts)  │    (fraud_velocity in DuckDB)
└──────────────────────┘
        ↓
   Great Expectations + Airflow + Grafana Monitoring
```

---

## 1. Event Generation (Source)

A Python-based producer generates synthetic financial transactions using the **Faker** library. The producer validates the data against an **Avro schema** fetched from a schema registry before publishing messages to the `transactions-raw` Kafka topic. This ensures schema compatibility and prevents malformed data from entering the pipeline early.

---

## 2. Bronze Layer (Raw Ingestion)

A **PySpark Structured Streaming** job consumes the Kafka topic in real time and provides exactly-once processing guarantees through checkpointing.

- It appends an ingestion timestamp (`_ingested_at`) and writes the raw records into a **Delta Lake** table.
- This layer serves as the **immutable source of truth**. All raw events are preserved exactly as they arrived, allowing downstream issues to be debugged or reprocessed without data loss.
- Checkpoints are stored on local disk (or S3 in production deployments) to enable recovery after failures.

---

## 3. Silver Layer (Cleansed & Conformed)

A separate PySpark process continuously reads from the Bronze Delta table.

- It applies **schema enforcement**, **deduplicates** records on `transaction_id`, and writes the cleaned data to the Silver Delta table.
- Records that fail validation (e.g., missing required fields or schema violations) are routed to a **Dead Letter Queue (DLQ)** stored as a separate Delta table. The DLQ is monitored via Prometheus metrics and can be queried independently for investigation and reprocessing.

---

## 4. Gold Layer (Business Aggregates)

**dbt** transforms the Silver data into analytical models focused on fraud detection.

- It calculates rolling 1-hour transaction velocities and flags high-risk accounts.
- The final models are materialized in **DuckDB** for analytical querying.

---

## Failure Modes & Recovery

The architecture accounts for several common failure scenarios in streaming pipelines:

| Failure Mode | Recovery Strategy |
|--------------|-------------------|
| **Spark Checkpoint Corruption** | If a checkpoint becomes corrupted, the streaming job can be restarted from an earlier offset using Delta Lake time travel to reprocess data from a known good state. |
| **Kafka Retention Expiry** | If Bronze ingestion falls behind and messages expire in Kafka, the system relies on the immutable Bronze Delta Lake table as the source of truth instead of re-consuming from Kafka. |
| **DuckDB WAL Growth** | DuckDB’s write-ahead log is periodically checkpointed. In production, this would be monitored and old WAL files cleaned up to prevent unbounded disk growth. |
| **DLQ Accumulation** | The Dead Letter Queue is monitored through custom metrics. If the volume crosses a threshold, it triggers an alert so operators can investigate and reprocess failed records. |
| **Airflow Task Failures** | If dbt or Great Expectations tasks fail, Airflow prevents downstream tasks from running and sends notifications, ensuring bad data does not reach serving layers. |

---

## Engineering Trade-offs

### 1. Delta Lake vs. Standard Parquet

**Delta Lake** was chosen over plain Parquet for the Bronze and Silver layers.

- **Benefit:** It provides ACID transactions, schema evolution, and time travel capabilities on top of object storage. These features are especially valuable in streaming pipelines where concurrent reads, writes, and late-arriving data are common.
- **Trade-off:** Delta Lake maintains a `_delta_log` directory, which increases the total number of files and overall storage footprint compared to plain Parquet.

### 2. PySpark Structured Streaming vs. Lightweight Consumers

**PySpark Structured Streaming** was selected to move data from Kafka into the lakehouse instead of lightweight Python consumers or Kafka Connect.

- **Benefit:** It offers native exactly-once processing semantics through checkpointing. If the streaming job fails or restarts, it resumes from the last successfully processed offset, preventing duplicate or lost records.
- **Trade-off:** Spark has higher resource overhead (JVM) and a steeper learning curve compared to lightweight Python-based consumers.

### 3. DuckDB + dbt vs. Cloud Data Warehouses

The Gold layer uses **dbt with DuckDB** as the execution engine instead of a managed cloud data warehouse such as Snowflake or BigQuery.

- **Benefit:** DuckDB provides a full SQL engine with native support for complex window functions, joins, and aggregations directly on Parquet files. It integrates cleanly with dbt, allowing analytical transformations to be expressed as maintainable SQL models rather than Python code, while incurring zero cloud infrastructure cost.
- **Trade-off:** DuckDB is an in-process, single-node database. It does not support horizontal scaling across multiple nodes for very large-scale analytics.

### 4. Data Quality as a Contract (Great Expectations)

**Great Expectations** is used to validate data quality on the Gold layer rather than relying only on implicit database constraints.

- **Benefit:** It enables declarative data contracts, automatically generates documentation, and can act as a pipeline gate. If critical business rules are violated, the pipeline can be halted before bad data reaches downstream systems.
- **Trade-off:** It requires upfront effort to define and maintain expectation suites.

### 5. Orchestration vs. Streaming Isolation

**Apache Airflow** is used as the orchestration layer, but it does not run the continuous streaming jobs.

- PySpark Structured Streaming runs as long-lived, independent processes.
- Airflow is responsible only for batch-oriented tasks such as dbt transformations and Great Expectations validations.

- **Benefit:** This separation prevents failures in analytical workloads from interrupting the continuous ingestion of raw transaction data, improving overall system resilience.
- **Trade-off:** It adds operational complexity, as two different execution models (streaming and batch orchestration) must be managed and monitored.
