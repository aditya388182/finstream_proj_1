# System Architecture & Design Decisions

FinStream is a real-time data pipeline that ingests synthetic financial transactions and processes them through a Medallion Architecture (**Bronze → Silver → Gold**).

This document explains the data flow, component choices, and key engineering trade-offs made during its design.

---

# Data Flow (Medallion Architecture)

The pipeline follows a strict three-layer structure to maintain data immutability, enforce schema quality, and deliver analytics-ready data.

```text
Producer (Faker + Avro)
        ↓
Apache Kafka + Schema Registry
        ↓
┌─────────────────────────────┐
│       Bronze Layer          │
│     (raw transactions)      │
│ PySpark Streaming + Delta   │
└─────────────────────────────┘
        ↓
┌─────────────────────────────┐
│       Silver Layer          │
│   (cleaned & validated)     │
│ PySpark + Delta Lake        │
└─────────────────────────────┘
        ↓
┌─────────────────────────────┐
│         Gold Layer          │
│     (analytics marts)       │
│      dbt + DuckDB           │
└─────────────────────────────┘
        ↓
Great Expectations + Airflow + Grafana
```

---

# 1. Event Generation (Source)

A Python-based producer generates synthetic financial transactions using the Faker library.

Before publishing to the `transactions-raw` Kafka topic, the producer validates each payload against a strict Avro schema fetched dynamically from a containerized Confluent Schema Registry.

## Why This Matters

This acts as a strict data contract at the source.

Benefits include:

- Prevention of malformed data
- Guaranteed schema compatibility
- Controlled schema evolution
- Reduced network payload size compared to JSON

---

# 2. Bronze Layer (Raw Ingestion)

A PySpark Structured Streaming job consumes the Kafka topic in real time.

## Responsibilities

- Consume raw Kafka events
- Append an ingestion timestamp (`_ingested_at`)
- Write records into a Delta Lake table
- Maintain Spark checkpoints

## Why Bronze Exists

The Bronze layer acts as the immutable historical audit trail.

All raw events are preserved exactly as received, allowing:

- Historical replay
- Debugging
- Backfills
- Recovery from downstream failures

## Reliability

Checkpoint data is stored locally.

If the streaming container restarts, Spark resumes processing from the exact Kafka offset where it previously stopped.

---

# 3. Silver Layer (Cleansed & Conformed)

A second PySpark process continuously reads from the Bronze Delta table.

## Responsibilities

- Schema enforcement and casting
- Deduplication using `transaction_id`
- Routing invalid records to a DLQ

## Dead Letter Queue (DLQ)

Records that fail validation (for example, missing critical fields) are written to a separate Delta table.

This prevents malformed records from crashing the primary streaming pipeline while preserving them for later investigation.

---

# 4. Gold Layer (Business Aggregates)

dbt transforms Silver-layer data into analytical models focused on fraud detection.

## Responsibilities

- Rolling transaction velocity calculations
- Fraud-risk flag generation
- Business-oriented analytical marts

## Serving Layer

The final models are materialized in DuckDB.

Benefits include:

- Extremely fast analytical queries
- SQL-based transformations
- Zero cloud infrastructure cost

---

# Failure Modes & Recovery

The architecture relies primarily on built-in recovery mechanisms provided by Spark, Delta Lake, Airflow, and Kafka.

| Failure Mode | Recovery Strategy |
|--------------|------------------|
| Spark container crash | Spark checkpointing allows immediate recovery from the last committed Kafka offset after container restart. |
| Kafka retention expiry | Bronze Delta Lake acts as the permanent source of truth for historical data; Kafka expiry does not result in data loss. |
| Malformed data spikes | Invalid records are routed to the DLQ instead of terminating the streaming job. |
| Gold-layer logic errors | Great Expectations failures stop downstream Airflow execution, acting as a circuit breaker before serving bad data. |

---

# Engineering Trade-offs

## 1. Avro Serialization vs. JSON

Data is serialized into Avro format using Confluent Schema Registry before entering Kafka.

### Benefit

- Compact binary format
- Reduced network bandwidth usage
- Reduced storage requirements
- Strong schema typing
- Forward and backward compatibility

### Trade-off

- Additional infrastructure requirements
- Dedicated Schema Registry service
- Harder debugging because Kafka payloads are not human-readable

---

## 2. Delta Lake vs. Standard Parquet

### Benefit

Delta Lake provides:

- ACID transactions
- Schema evolution
- Time travel
- Safer concurrent reads and writes

These capabilities are especially valuable for streaming workloads.

### Trade-off

Delta Lake maintains a `_delta_log` directory which increases:

- File count
- Metadata overhead
- Storage footprint

compared to plain Parquet.

---

## 3. PySpark Structured Streaming vs. Lightweight Consumers

### Benefit

PySpark Structured Streaming provides:

- Exactly-once processing
- Stateful processing
- Checkpoint recovery
- Native Kafka integration

### Trade-off

Spark introduces:

- JVM overhead
- Increased memory consumption
- Greater operational complexity

compared to lightweight Python consumers.

---

## 4. DuckDB + dbt vs. Cloud Data Warehouses

### Benefit

DuckDB provides:

- Full SQL support
- Window functions
- Tight dbt integration
- Zero cloud cost

### Trade-off

DuckDB is a single-node analytical engine and does not scale horizontally like:

- Snowflake
- BigQuery
- Databricks SQL

---

## 5. Data Quality as a Contract (Great Expectations)

### Benefit

Great Expectations provides:

- Declarative data quality rules
- Automated validation
- Documentation generation
- Pipeline gating

### Trade-off

Expectation suites require:

- Significant initial setup
- Ongoing maintenance as business rules evolve

---

## 6. Orchestration vs. Streaming Isolation

Airflow orchestrates:

- dbt transformations
- Great Expectations validations

Airflow does **not** run the streaming ingestion jobs.

### Benefit

This separation prevents batch-processing failures from interrupting continuous data ingestion.

### Trade-off

Two distinct execution models must be operated simultaneously:

1. Long-running streaming services
2. Scheduled batch workflows

---

# Future Architecture Capabilities

The following enhancements are not implemented today but represent a roadmap toward an enterprise-grade deployment.

## Cloud Managed Infrastructure

Migrate from local Docker containers to managed services such as:

- AWS MSK (Kafka)
- Databricks (Spark)
- Snowflake (Analytics)

## Automated DLQ Alerting

Implement custom Prometheus exporters around the Dead Letter Queue to:

- Detect malformed-data spikes
- Generate PagerDuty alerts
- Generate Slack notifications

## Active Alerting Sinks

Publish high-risk fraud events from the Gold layer into a dedicated Kafka topic for:

- Real-time notification systems
- Downstream fraud workflows
- Event-driven integrations

---

# Key Design Principle

**Bronze preserves data, Silver enforces quality, and Gold delivers business value.**

This separation keeps ingestion resilient, transformations maintainable, and analytics reproducible.
