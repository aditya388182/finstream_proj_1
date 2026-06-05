# System Architecture & Design Decisions

FinStream is a real-time data pipeline that ingests synthetic financial transactions and processes them through a Medallion Architecture (**Bronze → Silver → Gold**). This document explains the data flow, component choices, and key engineering trade-offs made during its design.

---

## Data Flow (Medallion Architecture)

The pipeline follows a strict three-layer structure to maintain data immutability, enforce schema quality, and deliver analytics-ready data.

```text
Producer (Faker + JSON)
        ↓
Apache Kafka (Docker)
        ↓
┌─────────────────────────────┐
│        Bronze Layer         │
│      (raw transactions)     │
│ PySpark Streaming + Delta   │
└─────────────────────────────┘
        ↓
┌─────────────────────────────┐
│        Silver Layer         │
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

## 1. Event Generation (Source)

A Python-based producer generates synthetic financial transactions using the Faker library.

The producer:

- Creates synthetic transaction records
- Formats events as JSON payloads
- Publishes records continuously to the `transactions-raw` Kafka topic

---

## 2. Bronze Layer (Raw Ingestion)

A PySpark Structured Streaming job consumes the Kafka topic in real time.

### Responsibilities

- Consumes raw Kafka events
- Appends an ingestion timestamp (`_ingested_at`)
- Writes records into a Delta Lake table
- Maintains Spark checkpoints

### Why Bronze Exists

The Bronze layer acts as the immutable historical audit trail.

All raw events are preserved exactly as received, allowing:

- Historical replay
- Debugging
- Backfills
- Recovery from downstream failures

### Reliability

Checkpoint data is stored on local disk.

If the streaming container restarts, Spark resumes processing from the last committed offset.

---

## 3. Silver Layer (Cleansed & Conformed)

A second PySpark process continuously reads from the Bronze Delta table.

### Responsibilities

- Schema enforcement
- Data validation
- Deduplication using `transaction_id`
- Routing invalid records to a DLQ

### Dead Letter Queue (DLQ)

Records that fail validation (for example, missing critical fields) are written to a separate Delta table.

This prevents malformed records from crashing the main streaming pipeline while preserving them for investigation.

---

## 4. Gold Layer (Business Aggregates)

dbt transforms Silver-layer data into analytical models focused on fraud detection.

### Responsibilities

- Rolling transaction velocity calculations
- Fraud-risk flag generation
- Business-oriented analytical marts

### Serving Layer

The final models are materialized in DuckDB, providing:

- Fast analytical queries
- SQL-based transformations
- Zero cloud infrastructure cost

---

# Failure Modes & Recovery

The architecture relies primarily on built-in recovery mechanisms provided by Spark, Delta Lake, Airflow, and Kafka.

| Failure Mode | Recovery Strategy |
|-------------|------------------|
| Spark container crash | Spark checkpointing allows recovery from the last committed Kafka offset after container restart. |
| Kafka retention expiry | Bronze Delta Lake acts as the permanent source of truth for historical data. |
| Malformed data spikes | Invalid records are routed to the DLQ instead of terminating the streaming job. |
| Gold-layer logic errors | Great Expectations failures stop downstream Airflow execution before serving bad data. |

---

# Engineering Trade-offs

## 1. Delta Lake vs. Standard Parquet

### Benefit

Delta Lake provides:

- ACID transactions
- Schema evolution
- Time travel
- Safer concurrent reads and writes

These features are especially valuable for streaming pipelines.

### Trade-off

Delta Lake maintains a `_delta_log` directory, increasing:

- File count
- Metadata overhead
- Storage footprint

compared to plain Parquet.

---

## 2. PySpark Structured Streaming vs. Lightweight Consumers

### Benefit

PySpark Structured Streaming provides:

- Native Kafka integration
- Exactly-once semantics
- Checkpoint-based recovery

This significantly reduces duplicate-processing risk.

### Trade-off

Spark introduces:

- JVM overhead
- More memory consumption
- Greater operational complexity

than lightweight Python consumers.

---

## 3. DuckDB + dbt vs. Cloud Data Warehouses

### Benefit

DuckDB provides:

- Full SQL support
- Window functions
- Joins and aggregations
- Tight dbt integration
- Zero cloud cost

This makes it ideal for portfolio-scale analytics projects.

### Trade-off

DuckDB is a single-node analytical engine and does not horizontally scale like:

- Snowflake
- BigQuery
- Redshift

---

## 4. Data Quality as a Contract (Great Expectations)

### Benefit

Great Expectations enables:

- Declarative data quality rules
- Automated validation
- Documentation generation
- Pipeline gating

### Trade-off

Expectation suites require ongoing maintenance as business rules evolve.

---

## 5. Orchestration vs. Streaming Isolation

Airflow orchestrates:

- dbt transformations
- Great Expectations validations

Airflow **does not run** the streaming ingestion jobs.

### Benefit

This separation prevents analytical task failures from interrupting continuous data ingestion.

### Trade-off

Two execution models must be managed:

1. Long-running streaming services
2. Scheduled batch workflows

This increases operational complexity.

---

# Future Architecture Capabilities

The following enhancements are **not implemented today**, but represent potential future evolution toward an enterprise-grade deployment.

### Confluent Schema Registry (Avro)

Move from JSON to Avro serialization to:

- Enforce schema compatibility
- Reduce payload size
- Improve producer/consumer contract management

### Automated DLQ Alerting

Implement custom monitoring around the Dead Letter Queue to:

- Detect malformed-data spikes
- Generate operational alerts
- Reduce manual monitoring effort

### Active Alerting Sinks

Publish high-risk fraud events from the Gold layer back into Kafka for:

- Real-time notification systems
- Downstream alerting services
- Event-driven fraud workflows

---

## Key Design Principle

**Bronze preserves data, Silver enforces quality, and Gold delivers business value.**

This separation keeps ingestion resilient, transformations maintainable, and analytics reproducible.
