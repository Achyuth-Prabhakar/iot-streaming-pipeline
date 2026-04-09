"""
Kafka Consumer
--------------
Reads IoT events from Kafka/Azure Event Hubs, validates, loads to Snowflake.
Uses kafka-python-ng for local Kafka.
Uses azure-eventhub SDK for Azure Event Hubs.
"""

import json
import os
import time
import uuid
import logging
from datetime import datetime
from validator import validate_event

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [CONSUMER] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC             = os.getenv('KAFKA_TOPIC', 'iot-sensor-events')
BATCH_SIZE              = int(os.getenv('BATCH_SIZE', '500'))
BATCH_TIMEOUT_SECS      = int(os.getenv('BATCH_TIMEOUT_SECS', '300'))
CONSUMER_GROUP          = os.getenv('CONSUMER_GROUP', '$Default')


def consume_and_validate():
    """Main consumer loop — detects Azure vs local and uses appropriate client."""
    from loader import SnowflakeLoader

    connection_string = os.getenv('KAFKA_CONNECTION_STRING', '')

    if connection_string:
        consume_azure(connection_string)
    else:
        consume_local()


def consume_azure(connection_string: str):
    """Consume from Azure Event Hubs using azure-eventhub SDK."""
    from azure.eventhub import EventHubConsumerClient
    from loader import SnowflakeLoader

    batch_id       = str(uuid.uuid4())
    loader         = SnowflakeLoader()
    loader.ensure_tables_exist()

    valid_batch    = []
    rejected_batch = []
    total_consumed = 0
    total_valid    = 0
    total_rejected = 0
    start_time     = time.time()

    logger.info(f"Starting Azure batch {batch_id[:8]} | Timeout: {BATCH_TIMEOUT_SECS}s")

    def on_event(partition_context, event):
        nonlocal total_consumed, total_valid, total_rejected

        raw = event.body_as_str()
        try:
            ev = json.loads(raw)
        except json.JSONDecodeError:
            partition_context.update_checkpoint(event)
            return

        result = validate_event(ev)
        total_consumed += 1

        if result.is_valid:
            ev.pop('_error_injected', None)
            valid_batch.append(ev)
            total_valid += 1
        else:
            rejected_batch.append({
                'event_id':        ev.get('event_id', 'UNKNOWN'),
                'machine_id':      ev.get('machine_id', 'UNKNOWN'),
                'raw_payload':     raw,
                'error_code':      result.error_code,
                'error_detail':    result.error_detail,
                'rejected_at':     datetime.utcnow().isoformat() + 'Z',
                'kafka_offset':    event.sequence_number,
                'kafka_partition': int(partition_context.partition_id),
            })
            total_rejected += 1

        # Flush mid-batch
        if len(valid_batch) >= BATCH_SIZE:
            loader.insert_valid_events(valid_batch, batch_id=batch_id)
            valid_batch.clear()
        if len(rejected_batch) >= BATCH_SIZE:
            loader.insert_rejected_events(rejected_batch, batch_id=batch_id)
            rejected_batch.clear()

        partition_context.update_checkpoint(event)

    client = EventHubConsumerClient.from_connection_string(
        connection_string,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=KAFKA_TOPIC,
    )

    logger.info(f"Connected to Azure Event Hubs | Group: {CONSUMER_GROUP}")

    with client:
        client.receive(
            on_event=on_event,
            starting_position="@latest",
            max_wait_time=BATCH_TIMEOUT_SECS,
        )

    # Final flush
    if valid_batch:
        loader.insert_valid_events(valid_batch, batch_id=batch_id)
        logger.info(f"Flushed {len(valid_batch)} valid events")
    if rejected_batch:
        loader.insert_rejected_events(rejected_batch, batch_id=batch_id)
        logger.info(f"Flushed {len(rejected_batch)} rejected events")

    elapsed = time.time() - start_time
    rate    = total_consumed / elapsed if elapsed > 0 else 0

    if total_consumed > 0:
        loader.log_pipeline_metrics(
            batch_id        = batch_id,
            total_consumed  = total_consumed,
            total_valid     = total_valid,
            total_rejected  = total_rejected,
            records_per_sec = round(rate, 2)
        )

    logger.info(
        f"Batch complete | consumed: {total_consumed} | "
        f"valid: {total_valid} | rejected: {total_rejected} | "
        f"rate: {rate:.1f}/sec"
    )
    loader.close()


def consume_local():
    """Consume from local Kafka using kafka-python-ng."""
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    from loader import SnowflakeLoader

    batch_id       = str(uuid.uuid4())
    loader         = SnowflakeLoader()
    loader.ensure_tables_exist()

    valid_batch    = []
    rejected_batch = []
    total_consumed = 0
    total_valid    = 0
    total_rejected = 0
    start_time     = time.time()

    for attempt in range(1, 16):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=CONSUMER_GROUP,
                max_poll_records=500,
                session_timeout_ms=30000,
                consumer_timeout_ms=BATCH_TIMEOUT_SECS * 1000,
            )
            logger.info(f"Connected to local Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            break
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready. Attempt {attempt}/15. Retrying in 10s...")
            time.sleep(10)
    else:
        raise RuntimeError("Could not connect to Kafka after max retries")

    logger.info(f"Starting local batch {batch_id[:8]} | Timeout: {BATCH_TIMEOUT_SECS}s")

    for message in consumer:
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
                'kafka_offset':    message.offset,
                'kafka_partition': message.partition,
            })
            total_rejected += 1

        if len(valid_batch) >= BATCH_SIZE:
            loader.insert_valid_events(valid_batch, batch_id=batch_id)
            valid_batch = []
        if len(rejected_batch) >= BATCH_SIZE:
            loader.insert_rejected_events(rejected_batch, batch_id=batch_id)
            rejected_batch = []

    if valid_batch:
        loader.insert_valid_events(valid_batch, batch_id=batch_id)
    if rejected_batch:
        loader.insert_rejected_events(rejected_batch, batch_id=batch_id)

    elapsed = time.time() - start_time
    rate    = total_consumed / elapsed if elapsed > 0 else 0

    if total_consumed > 0:
        loader.log_pipeline_metrics(
            batch_id        = batch_id,
            total_consumed  = total_consumed,
            total_valid     = total_valid,
            total_rejected  = total_rejected,
            records_per_sec = round(rate, 2)
        )

    logger.info(
        f"Batch complete | consumed: {total_consumed} | "
        f"valid: {total_valid} | rejected: {total_rejected}"
    )
    consumer.close()
    loader.close()


if __name__ == '__main__':
    consume_and_validate()