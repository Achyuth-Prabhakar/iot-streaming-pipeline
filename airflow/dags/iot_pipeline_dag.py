"""
IoT Streaming Pipeline — Airflow DAG
--------------------------------------
Orchestrates the full pipeline every 5 minutes:

  Task 1: health_check       → verify Kafka + Snowflake reachable
  Task 2: consume_and_load   → read from Kafka, validate, load to Snowflake
  Task 3: log_metrics        → write run summary to PIPELINE_METRICS
  Task 4: quality_report     → query Snowflake, log the 92% metric

Schedule: every 5 minutes
Catchup:  False (don't backfill missed runs)
"""

import sys
import os
import json
import uuid
import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add consumer module to path so we can import validator + loader
sys.path.insert(0, '/opt/airflow/consumer')

# ── Default DAG args ──────────────────────────────────────────────────────────
default_args = {
    'owner':            'achyuth',
    'retries':          2,
    'retry_delay':      timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=4),
}

# ── Environment ───────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC             = os.getenv('KAFKA_TOPIC', 'iot-sensor-events')
BATCH_CONSUME_SECONDS   = int(os.getenv('BATCH_CONSUME_SECONDS', '60'))  # read for 60s per run
MAX_RECORDS_PER_RUN     = int(os.getenv('MAX_RECORDS_PER_RUN', '500'))   # cap per DAG run


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — Health Check
# ─────────────────────────────────────────────────────────────────────────────
def health_check(**context):
    """
    Verify Kafka and Snowflake are reachable before doing any work.
    Fails fast so downstream tasks don't run against a broken dependency.
    """
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    import snowflake.connector

    errors = []

    # Check Kafka
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            consumer_timeout_ms=5000,
        )
        topics = consumer.topics()
        consumer.close()
        logging.info(f"✅ Kafka healthy — topics visible: {len(topics)}")
    except Exception as e:
        errors.append(f"Kafka unreachable: {e}")

    # Check Snowflake
    try:
        conn = snowflake.connector.connect(
            account   = os.getenv('SNOWFLAKE_ACCOUNT'),
            user      = os.getenv('SNOWFLAKE_USER'),
            password  = os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
            database  = os.getenv('SNOWFLAKE_DATABASE'),
            schema    = os.getenv('SNOWFLAKE_SCHEMA'),
            role      = os.getenv('SNOWFLAKE_ROLE'),
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        logging.info("✅ Snowflake healthy")
    except Exception as e:
        errors.append(f"Snowflake unreachable: {e}")

    if errors:
        raise RuntimeError(f"Health check failed: {errors}")

    # Pass batch_id to downstream tasks via XCom
    batch_id = str(uuid.uuid4())
    context['ti'].xcom_push(key='batch_id', value=batch_id)
    logging.info(f"✅ Health check passed — batch_id: {batch_id}")
    return batch_id


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — Consume and Load
# ─────────────────────────────────────────────────────────────────────────────
def consume_and_load(**context):
    """
    Read events from Kafka for BATCH_CONSUME_SECONDS seconds,
    validate each one, and load to Snowflake in batches.
    Returns metrics dict via XCom.
    """
    from kafka import KafkaConsumer
    from validator import validate_event
    from loader import SnowflakeLoader

    batch_id = context['ti'].xcom_pull(key='batch_id', task_ids='health_check')
    loader   = SnowflakeLoader()
    loader.ensure_tables_exist()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',       # only new messages each run
        enable_auto_commit=True,
        group_id='airflow-pipeline-group',
        consumer_timeout_ms=BATCH_CONSUME_SECONDS * 1000,
        max_poll_records=100,
    )

    valid_batch    = []
    rejected_batch = []
    total_consumed = 0
    total_valid    = 0
    total_rejected = 0
    start_time     = time.time()

    logging.info(
        f"🚀 Starting consume loop — "
        f"batch_id: {batch_id[:8]}... | "
        f"reading for {BATCH_CONSUME_SECONDS}s | "
        f"max: {MAX_RECORDS_PER_RUN} records"
    )

    for message in consumer:
        if total_consumed >= MAX_RECORDS_PER_RUN:
            break

        event  = message.value
        result = validate_event(event)
        total_consumed += 1

        if result.is_valid:
            event.pop('_error_injected', None)
            valid_batch.append(event)
            total_valid += 1
        else:
            rejected_batch.append({
                'event_id':        event.get('event_id', 'UNKNOWN'),
                'machine_id':      event.get('machine_id', 'UNKNOWN'),
                'raw_payload':     json.dumps(event),
                'error_code':      result.error_code,
                'error_detail':    result.error_detail,
                'rejected_at':     datetime.utcnow().isoformat() + 'Z',
                'kafka_partition': message.partition,
                'kafka_offset':    message.offset,
            })
            total_rejected += 1

        # Flush to Snowflake every 100 records
        if len(valid_batch) >= 100:
            loader.insert_valid_events(valid_batch, batch_id=batch_id)
            valid_batch = []

        if len(rejected_batch) >= 100:
            loader.insert_rejected_events(rejected_batch, batch_id=batch_id)
            rejected_batch = []

    # Final flush
    if valid_batch:
        loader.insert_valid_events(valid_batch, batch_id=batch_id)
    if rejected_batch:
        loader.insert_rejected_events(rejected_batch, batch_id=batch_id)

    loader.close()
    consumer.close()

    elapsed = time.time() - start_time
    rate    = total_consumed / elapsed if elapsed > 0 else 0

    metrics = {
        'batch_id':        batch_id,
        'total_consumed':  total_consumed,
        'total_valid':     total_valid,
        'total_rejected':  total_rejected,
        'records_per_sec': round(rate, 2),
        'elapsed_sec':     round(elapsed, 2),
    }

    logging.info(
        f"✅ Consume complete — "
        f"consumed: {total_consumed} | "
        f"valid: {total_valid} | "
        f"rejected: {total_rejected} | "
        f"rate: {rate:.1f}/sec"
    )

    context['ti'].xcom_push(key='metrics', value=metrics)
    return metrics


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — Log Metrics
# ─────────────────────────────────────────────────────────────────────────────
def log_metrics(**context):
    """Write pipeline run metrics to PIPELINE_METRICS table."""
    from loader import SnowflakeLoader

    metrics = context['ti'].xcom_pull(key='metrics', task_ids='consume_and_load')

    if not metrics:
        logging.warning("No metrics received from consume_and_load — skipping")
        return

    loader = SnowflakeLoader()
    loader.log_pipeline_metrics(
        batch_id        = metrics['batch_id'],
        total_consumed  = metrics['total_consumed'],
        total_valid     = metrics['total_valid'],
        total_rejected  = metrics['total_rejected'],
        records_per_sec = metrics['records_per_sec'],
        status          = 'SUCCESS',
    )
    loader.close()
    logging.info(f"✅ Metrics logged for batch {metrics['batch_id'][:8]}...")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — Data Quality Report
# ─────────────────────────────────────────────────────────────────────────────
def data_quality_report(**context):
    """
    Query Snowflake for cumulative pipeline stats.
    Logs the 92% metric across all runs — this is the proof.
    """
    import snowflake.connector

    conn = snowflake.connector.connect(
        account   = os.getenv('SNOWFLAKE_ACCOUNT'),
        user      = os.getenv('SNOWFLAKE_USER'),
        password  = os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
        database  = os.getenv('SNOWFLAKE_DATABASE'),
        schema    = os.getenv('SNOWFLAKE_SCHEMA'),
        role      = os.getenv('SNOWFLAKE_ROLE'),
    )
    cur = conn.cursor()

    # Cumulative stats across all pipeline runs
    cur.execute("""
        SELECT
            SUM(TOTAL_CONSUMED)                                        AS total_consumed,
            SUM(TOTAL_VALID)                                           AS total_valid,
            SUM(TOTAL_REJECTED)                                        AS total_rejected,
            ROUND(AVG(VALID_PCT), 2)                                   AS avg_valid_pct,
            ROUND(AVG(REJECTED_PCT), 2)                                AS avg_rejected_pct,
            ROUND(AVG(RECORDS_PER_SECOND), 2)                          AS avg_rps,
            COUNT(*)                                                   AS total_runs
        FROM PIPELINE_METRICS
        WHERE PIPELINE_STATUS = 'SUCCESS'
    """)
    row = cur.fetchone()

    if row and row[0]:
        logging.info("=" * 55)
        logging.info("📊 CUMULATIVE PIPELINE QUALITY REPORT")
        logging.info("=" * 55)
        logging.info(f"  Total runs         : {row[6]}")
        logging.info(f"  Total consumed     : {row[0]:,}")
        logging.info(f"  Total valid        : {row[1]:,}")
        logging.info(f"  Total rejected     : {row[2]:,}")
        logging.info(f"  Avg valid %%        : {row[3]}%%")
        logging.info(f"  Avg rejected %%     : {row[4]}%%")
        logging.info(f"  Avg records/sec    : {row[5]}")
        logging.info("=" * 55)
        logging.info(
            f"  ✅ Pipeline maintaining {row[3]}%% data quality "
            f"(target: 92%%)"
        )

    # Also check total rows in each table
    cur.execute("SELECT COUNT(*) FROM IOT_EVENTS")
    valid_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM IOT_EVENTS_REJECTED")
    rejected_count = cur.fetchone()[0]

    logging.info(f"  📦 IOT_EVENTS rows        : {valid_count:,}")
    logging.info(f"  🗑️  IOT_EVENTS_REJECTED rows: {rejected_count:,}")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id            = 'iot_streaming_pipeline',
    default_args      = default_args,
    description       = 'Real-time IoT pipeline: Kafka → Validate → Snowflake',
    schedule_interval = '*/5 * * * *',   # every 5 minutes
    start_date        = datetime(2026, 1, 1),
    catchup           = False,
    max_active_runs   = 1,               # prevent overlapping runs
    tags              = ['iot', 'kafka', 'snowflake', 'streaming'],
) as dag:

    t1_health_check = PythonOperator(
        task_id         = 'health_check',
        python_callable = health_check,
    )

    t2_consume = PythonOperator(
        task_id         = 'consume_and_load',
        python_callable = consume_and_load,
    )

    t3_metrics = PythonOperator(
        task_id         = 'log_metrics',
        python_callable = log_metrics,
    )

    t4_report = PythonOperator(
        task_id         = 'data_quality_report',
        python_callable = data_quality_report,
    )

    # Task dependency chain
    
    t1_health_check >> t2_consume >> t3_metrics >> t4_report