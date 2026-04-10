# Real-Time IoT Streaming Pipeline

End-to-end streaming data pipeline that ingests sensor events from simulated IoT devices in real-time, validates data quality across 9 rules, and loads clean records into Snowflake for downstream analytics. Deployed both locally via Docker and on Azure.

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Daily throughput | 500,000+ records/day |
| Data quality | 92% clean records reaching warehouse |
| Latency | < 5 minutes raw event to Snowflake |
| Machines simulated | 20 industrial machines |
| Validation rules | 9 quality checks |
| Azure Event Hubs messages (24h) | 517,000+ |

---

## Architecture

### Phase 1 — Local (Docker)

```
IoT Producer (Python)
  - 20 machines, 6 events/sec
  - Statistical distributions from AI4I 2020 dataset
  - 8% intentional error injection
        |
        v
Apache Kafka (KRaft mode — no Zookeeper)
  - Topic: iot-sensor-events
  - 3 partitions, partition key = machine_id
  - 24h retention
        |
        v
Data Quality Validator (9 rules)
  - Missing fields, null values
  - Timestamp format
  - Machine ID/type validation
  - Numeric type checks
  - Physical bounds (RPM, temperature, torque)
  - Physics consistency (process temp vs air temp)
  - Deduplication within 1-hour window
        |
        |--- Valid (92%) -----> Snowflake IOT_EVENTS
        |--- Invalid (8%) ----> Snowflake IOT_EVENTS_REJECTED
        |
        v
Apache Airflow DAG (every 5 minutes)
  health_check -> consume_and_load -> log_metrics -> data_quality_report
        |
        v
Snowflake
  - IOT_EVENTS
  - IOT_EVENTS_REJECTED
  - PIPELINE_METRICS
```

### Phase 2 — Azure

```
IoT Producer (Azure Container Instance)
        |
        v
Azure Event Hubs (Kafka-compatible endpoint)
  - SASL/SSL authentication
  - Same topic structure as local Kafka
        |
        v
Consumer (Azure Container Instance)
  - azure-eventhub SDK
  - Same validation logic
  - Batched Snowflake loading
        |
        v
Snowflake (same instance, new rows tagged by date)
```

---

## Tech Stack

| Layer | Local | Azure |
|-------|-------|-------|
| Event Streaming | Apache Kafka (KRaft) | Azure Event Hubs |
| Orchestration | Apache Airflow 2.8.1 | Apache Airflow |
| Data Warehouse | Snowflake | Snowflake |
| Containerization | Docker Compose | Azure Container Instances |
| Container Registry | Local | Azure Container Registry |
| Secrets | .env file | Azure Key Vault |
| Language | Python 3.11 | Python 3.11 |

---

## Project Structure

```
iot-streaming-pipeline/
├── producer/
│   ├── producer.py          # IoT event simulator
│   ├── Dockerfile
│   └── requirements.txt
├── consumer/
│   ├── consumer.py          # Kafka/Event Hubs consumer
│   ├── validator.py         # 9 data quality rules
│   ├── loader.py            # Snowflake writer
│   ├── smoke_test_consumer.py
│   ├── test_validator.py
│   ├── test_loader.py
│   └── Dockerfile
├── airflow/
│   └── dags/
│       └── iot_pipeline_dag.py
├── snowflake/
│   └── schema.sql
├── docker-compose.yml
└── .env.example
```

---

## Local Setup

**Prerequisites:** Docker Desktop, Python 3.11+

```bash
git clone https://github.com/Achyuth-Prabhakar/iot-streaming-pipeline.git
cd iot-streaming-pipeline

cp .env.example .env
# Fill in your Snowflake credentials in .env

docker compose up -d
```

Services started:
- Kafka UI: http://localhost:8080
- Airflow: http://localhost:8081 (admin / admin123)

**Run validation smoke test:**
```bash
cd consumer
python smoke_test_consumer.py
```

Expected output:
```
Total sampled  : 50
Valid          : 46  (92.0%)
Rejected       : 4   (8.0%)
Validator is catching ~8% of bad records
```

---

## Data Quality Rules

The validator (`consumer/validator.py`) runs 9 rules against every event:

1. **Required fields present** — checks all 9 fields exist in the event dict
2. **No null values** — critical fields cannot be None
3. **Valid ISO 8601 timestamp** — regex + datetime.fromisoformat()
4. **Machine ID format** — must match MACHINE_NNN (3 digits)
5. **Machine type** — must be L, M, or H
6. **Numeric types** — sensor fields must be castable to float
7. **Sensor bounds** — RPM 100-3000, temperature 270-350K, torque 0.1-100Nm
8. **Physics consistency** — process temp cannot be 5K below air temp
9. **Deduplication** — event_id unique within 1-hour sliding window

Records failing any rule go to `IOT_EVENTS_REJECTED` with `ERROR_CODE` and `ERROR_DETAIL` logged.

---

## Snowflake Schema

```sql
-- Clean validated records
IOT_EVENTS (
    EVENT_ID, MACHINE_ID, MACHINE_TYPE, EVENT_TIMESTAMP,
    AIR_TEMPERATURE_K, PROCESS_TEMPERATURE_K, ROTATIONAL_SPEED_RPM,
    TORQUE_NM, TOOL_WEAR_MIN, POWER_W, MACHINE_FAILURE, FAILURE_TYPE,
    INGESTED_AT, PIPELINE_BATCH_ID
)

-- Rejected records with full audit trail
IOT_EVENTS_REJECTED (
    REJECTION_ID, EVENT_ID, MACHINE_ID, RAW_PAYLOAD (VARIANT),
    ERROR_CODE, ERROR_DETAIL, REJECTED_AT, KAFKA_OFFSET, KAFKA_PARTITION
)

-- Per-run pipeline metrics
PIPELINE_METRICS (
    BATCH_ID, RUN_TIMESTAMP, TOTAL_CONSUMED, TOTAL_VALID,
    TOTAL_REJECTED, VALID_PCT, REJECTED_PCT, RECORDS_PER_SECOND, PIPELINE_STATUS
)
```

---

## Azure Deployment

```bash
# Resource group
az group create --name rg-iot-pipeline --location eastus2

# Event Hubs (Kafka-compatible)
az eventhubs namespace create --name iot-pipeline-eh-ns \
  --resource-group rg-iot-pipeline --sku Standard --enable-kafka true

az eventhubs eventhub create --name iot-sensor-events \
  --namespace-name iot-pipeline-eh-ns \
  --resource-group rg-iot-pipeline --partition-count 3

# Container Registry
az acr create --name iotpipelineacr \
  --resource-group rg-iot-pipeline --sku Basic --admin-enabled true

# Build and push images
docker build -t iotpipelineacr.azurecr.io/iot-producer:v2 ./producer
docker push iotpipelineacr.azurecr.io/iot-producer:v2

docker build -t iotpipelineacr.azurecr.io/iot-consumer:v8 ./consumer
docker push iotpipelineacr.azurecr.io/iot-consumer:v8

# Deploy containers
az container create --resource-group rg-iot-pipeline \
  --name iot-producer \
  --image iotpipelineacr.azurecr.io/iot-producer:v2 \
  --os-type Linux --restart-policy Always
```

**Key Azure finding:** `kafka-python-ng` connects to Azure Event Hubs but does not consume messages reliably. Switch to `azure-eventhub` SDK for the consumer in Azure deployments. The producer continues to use `kafka-python-ng` successfully.

---

## Known Issues and Fixes

### 1. kafka-python broken on Python 3.12+
**Error:** `ModuleNotFoundError: kafka.vendor.six.moves`
**Fix:** Uninstall `kafka-python`, install `kafka-python-ng`

### 2. Windows file extension issue
**Error:** `open Dockerfile: no such file or directory`
**Cause:** Windows saved file as `Dockerfile.txt`
**Fix:** Use PowerShell `Out-File` to create files without extensions

### 3. Snowflake 404 on connection
**Error:** `404 Not Found: post your_account_identifier.snowflakecomputing.com`
**Cause:** `.env` placeholder was never replaced
**Fix:** Set `SNOWFLAKE_ACCOUNT=iewaemw-fc94643` (from URL)

### 4. PARSE_JSON in VALUES clause
**Error:** `Invalid expression PARSE_JSON() in VALUES clause`
**Cause:** Snowflake connector blocks function calls in parameterized VALUES
**Fix:** Change `INSERT INTO...VALUES` to `INSERT INTO...SELECT`

### 5. Airflow db not initialized
**Error:** `No module named 'airflow'`
**Cause:** Init container ran as root but Airflow installed under user 50000
**Fix:** Set `user: "50000:0"` and prepend `export PATH=/home/airflow/.local/bin:$$PATH`

### 6. Docker Compose Windows PATH substitution
**Error:** `export PATH=/home/airflow...C:\Windows\System32...`
**Cause:** Docker Compose substituted Windows `$PATH`
**Fix:** Use `$$PATH` (double dollar) to escape variable substitution

### 7. ACR Tasks blocked on Valpo subscription
**Error:** `TasksOperationsNotAllowed`
**Fix:** Build images locally with Docker Desktop, push with `docker push`

### 8. ACI log streaming broken
**Error:** `InternalServerError` on `az container logs`
**Fix:** Use `az container attach` instead — streams live logs directly

### 9. Secret in git history
**Error:** GitHub push protection blocked push (Event Hubs key in `producer-deploy.yaml`)
**Fix:** Rotate keys first (`az eventhubs namespace authorization-rule keys renew`), then `git filter-branch` to remove file from history, then force push

### 10. kafka-python-ng zero consumption on Event Hubs
**Symptom:** Consumer connects successfully, authenticates, but `consumed: 0` every run
**Root cause:** `kafka-python-ng` consumer group offset management incompatible with Event Hubs
**Fix:** Switch to `azure-eventhub` SDK for Azure deployments. Producer continues using `kafka-python-ng` successfully

### 11. Hardcoded Windows path in loader.py
**Error:** Container crashes immediately with ExitCode 1
**Cause:** `load_dotenv(dotenv_path=r'C:\Users\eshae\...')` — path doesn't exist in Linux container
**Fix:** Change to `load_dotenv()` — searches current directory, skips silently if not found

### 12. consume_and_validate not defined
**Error:** `NameError: name 'consume_and_validate' is not defined`
**Cause:** Function was accidentally removed during SASL auth edits
**Fix:** Restore complete function definition before `if __name__ == '__main__':`

### 13. Secure environment variables null in ACI
**Symptom:** `KAFKA_CONNECTION_STRING: null` in container despite passing value
**Cause:** PowerShell strips special characters (`+`, `/`, `=`) from `--secure-environment-variables`
**Fix:** Quote the entire `KEY=VALUE` pair as a single string: `"KAFKA_CONNECTION_STRING=Endpoint=sb://..."`

### 14. Consumer group offset at end
**Symptom:** Consumer connects, runs 300s, `consumed: 0`
**Cause:** `auto_offset_reset: latest` with existing group = starts at current end, no historical messages
**Fix:** Use `$Default` consumer group (pre-created in Event Hubs) or create group in portal first

---

## Airflow DAG

```
health_check -> consume_and_load -> log_metrics -> data_quality_report
```

- **health_check** — verifies Kafka and Snowflake connectivity, generates batch_id via XCom
- **consume_and_load** — reads from Kafka for 60s, validates every event, flushes to Snowflake in batches of 100
- **log_metrics** — writes TOTAL_CONSUMED, VALID_PCT, REJECTED_PCT to PIPELINE_METRICS
- **data_quality_report** — queries cumulative stats, logs the 92% metric

Schedule: `*/5 * * * *` (every 5 minutes), `max_active_runs=1`

---

## Design Decisions

**KRaft over Zookeeper** — Kafka 4.0 removed Zookeeper entirely. KRaft is mandatory as of 2025. Eliminates a separate service.

**Partition key = Machine ID** — All events from the same machine go to the same partition in order. Critical for time-series reconstruction.

**Batched Snowflake loading** — Snowflake has ~100-200ms latency per call. Batching 500 records reduces round-trips by 500x.

**Fail-fast validation** — Stops at the first failing rule. Makes error codes precise and avoids unnecessary computation on bad records.

**azure-eventhub SDK for Azure** — kafka-python-ng works for producing to Event Hubs but has consumer group offset issues. The official SDK handles this correctly.

**Error injection at producer** — 8% of events are intentionally corrupted to generate realistic rejection data and prove the 92% metric is not fabricated.

---

## Results

```
Total pipeline runs    : 70+
Average VALID_PCT      : 92.4%
Average REJECTED_PCT   : 7.6%
Total events loaded    : 35,000+ (local) + 1,500+ (Azure)
Azure Event Hub (24h)  : 517,000+ messages
Pipeline status        : 100% SUCCESS
```

---

## Author

Achyuth Prabhakar Emidi
[achyuth-prabhakar.com](https://achyuth-prabhakar.com) | [LinkedIn](https://www.linkedin.com/in/achyuth-prabhakar) | [GitHub](https://github.com/Achyuth-Prabhakar)

MS Information Technology — Valparaiso University (Expected 2027)
