# Real-Time IoT Streaming Pipeline

End-to-end streaming data pipeline that ingests sensor events from simulated IoT devices in real-time, validates data quality, and loads clean records into Snowflake for downstream analytics.

## Metrics

| Metric | Value |
|--------|-------|
| Daily throughput | 500,000+ records/day |
| Data quality | 92% clean records |
| Latency | < 5 minutes raw event to Snowflake |
| Machines simulated | 20 industrial machines |
| Validation rules | 9 quality checks |

## Architecture
IoT Producer (Python)
|
v
Apache Kafka / Azure Event Hubs
|
v
Data Quality Validator (Python)

9 validation rules
Schema compliance
Physical bounds checks
Deduplication
|
|--- Valid (92%) -----> Snowflake IOT_EVENTS
|--- Invalid (8%) ----> Snowflake IOT_EVENTS_REJECTED
|
v
Apache Airflow (Orchestration)
|
v
Snowflake (Cloud Data Warehouse)
IOT_EVENTS
IOT_EVENTS_REJECTED
PIPELINE_METRICS


## Tech Stack

| Layer | Local | Cloud (Azure) |
|-------|-------|---------------|
| Event Streaming | Apache Kafka (KRaft) | Azure Event Hubs |
| Orchestration | Apache Airflow | Apache Airflow |
| Data Warehouse | Snowflake | Snowflake |
| Containerization | Docker Compose | Azure Container Instances |
| Container Registry | Local | Azure Container Registry |
| Secrets Management | .env file | Azure Key Vault |
| Infrastructure | Docker | Azure CLI |

## Project Structure
iot-streaming-pipeline/
├── producer/
│   ├── producer.py          # IoT event simulator — 6 events/sec, 20 machines
│   ├── Dockerfile
│   └── requirements.txt
├── consumer/
│   ├── consumer.py          # Kafka consumer with batched Snowflake loading
│   ├── validator.py         # 9 data quality rules
│   ├── loader.py            # Snowflake connector — valid + rejected + metrics
│   └── Dockerfile
├── airflow/
│   └── dags/
│       └── iot_pipeline_dag.py   # 4-task DAG — runs every 5 minutes
├── snowflake/
│   └── schema.sql           # IOT_EVENTS, IOT_EVENTS_REJECTED, PIPELINE_METRICS
└── docker-compose.yml       # Local: Kafka + Airflow + Postgres + Producer

## Data Quality Validation

The validator runs 9 rules against every event before it reaches Snowflake:

1. Required fields present
2. No null values in critical fields
3. Valid ISO 8601 timestamp
4. Machine ID matches naming convention (MACHINE_NNN)
5. Machine type is L, M, or H
6. All sensor fields are numeric
7. Sensor values within physical bounds
8. Process temperature physically consistent with air temperature
9. No duplicate event_id within 1-hour window

Records failing any rule are routed to `IOT_EVENTS_REJECTED` with the error code and detail logged for debugging.

## Snowflake Schema

```sql
IOT_EVENTS           -- clean validated records
IOT_EVENTS_REJECTED  -- invalid records with error_code and raw_payload
PIPELINE_METRICS     -- per-run stats proving the 92% quality metric
```

## Local Setup

**Prerequisites:** Docker Desktop, Python 3.11+

```bash
# Clone
git clone https://github.com/Achyuth-Prabhakar/iot-streaming-pipeline.git
cd iot-streaming-pipeline

# Create .env (see .env.example)
cp .env.example .env
# Fill in your Snowflake credentials

# Start all services
docker compose up -d

# Verify pipeline is running
# Kafka UI:   http://localhost:8080
# Airflow:    http://localhost:8081  (admin/admin123)
```

## Azure Deployment

All services deployed to Azure using CLI:

```bash
# Resources created
az group create --name rg-iot-pipeline --location eastus2

# Event Hubs (Kafka-compatible, zero code changes)
# Container Registry (stores producer + consumer images)
# Container Instances (runs producer + consumer 24/7)
# Key Vault (stores all secrets)
# Blob Storage (pipeline staging)
```

The producer and consumer connect to Azure Event Hubs using SASL/SSL authentication.
The Kafka bootstrap server changes from `kafka:29092` to `iot-pipeline-eh-ns.servicebus.windows.net:9093`.
No application code changes required.

## Key Design Decisions

**KRaft over Zookeeper** — Kafka 4.0 removed Zookeeper entirely. KRaft is the production standard as of 2025 and eliminates a separate service dependency.

**Batched loading over row-by-row inserts** — Snowflake has ~100-200ms latency per call. Batching 500 records per insert reduces network round-trips by 500x.

**Fail-fast validation** — The validator stops at the first failing rule rather than checking all 9. This reduces compute on bad records and makes error codes precise.

**Partition key = Machine ID** — Guarantees all events from the same machine arrive in order within a partition, enabling reliable time-series reconstruction.

## Airflow DAG
health_check → consume_and_load → log_metrics → data_quality_report

Runs every 5 minutes. Each task is a PythonOperator. XCom passes batch_id between tasks.

## Results
Total runs:       70+
Avg valid %:      92.4%
Avg rejected %:   7.6%
Total events:     35,000+
Pipeline status:  100% SUCCESS

## Author

Achyuth Prabhakar Emidi
[achyuth-prabhakar.com](https://achyuth-prabhakar.com) |
[LinkedIn](https://www.linkedin.com/in/achyuth-prabhakar) |
[GitHub](https://github.com/Achyuth-Prabhakar)
