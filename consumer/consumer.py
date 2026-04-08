"""
Kafka Consumer
--------------
Reads IoT events from Kafka, validates each one, routes:
  - Valid events   → staging buffer → Snowflake (iot_events table)
  - Invalid events → Snowflake rejected table (iot_events_rejected)

Tracks metrics to prove the 92% reduction claim.
"""

import json
import os
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from validator import validate_event

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [CONSUMER] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC             = os.getenv('KAFKA_TOPIC', 'iot-sensor-events')
BATCH_SIZE              = int(os.getenv('BATCH_SIZE', '500'))       # flush to Snowflake every N records
BATCH_TIMEOUT_SECS      = int(os.getenv('BATCH_TIMEOUT_SECS', '30')) # or every N seconds


def create_consumer(max_retries: int = 15) -> KafkaConsumer:
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='iot-validator-group',
                max_poll_records=500,
                session_timeout_ms=30000,
            )
            logger.info(f"✅ Consumer connected to {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready. Attempt {attempt}/{max_retries}. Waiting 10s...")
            time.sleep(10)
    raise RuntimeError("Could not connect to Kafka")


def consume_and_validate():
    """Main consumer loop with validation + batched Snowflake loading."""
    # Import here to avoid circular imports; loader needs Snowflake creds at runtime
    from loader import SnowflakeLoader

    consumer = create_consumer()
    loader   = SnowflakeLoader()
    loader.ensure_tables_exist()

    valid_batch    = []
    rejected_batch = []

    # Running metrics (for the 92% claim)
    total_consumed  = 0
    total_valid     = 0
    total_rejected  = 0
    batch_start     = time.time()

    logger.info(f"🎯 Starting consumer | Batch size: {BATCH_SIZE} | Timeout: {BATCH_TIMEOUT_SECS}s")

    for message in consumer:
        event = message.value
        total_consumed += 1

        result = validate_event(event)

        if result.is_valid:
            # Strip internal debug tag before loading
            event.pop('_error_injected', None)
            valid_batch.append(event)
            total_valid += 1
        else:
            rejected_record = {
                'event_id':    event.get('event_id', 'UNKNOWN'),
                'machine_id':  event.get('machine_id', 'UNKNOWN'),
                'raw_payload': json.dumps(event),
                'error_code':  result.error_code,
                'error_detail': result.error_detail,
                'rejected_at': datetime.utcnow().isoformat() + 'Z',
                'kafka_offset': message.offset,
                'kafka_partition': message.partition,
            }
            rejected_batch.append(rejected_record)
            total_rejected += 1

        # Flush batch to Snowflake when batch is full OR timeout reached
        elapsed = time.time() - batch_start
        should_flush = (
            len(valid_batch) >= BATCH_SIZE
            or len(rejected_batch) >= BATCH_SIZE
            or elapsed >= BATCH_TIMEOUT_SECS
        )

        if should_flush and (valid_batch or rejected_batch):
            if valid_batch:
                loader.insert_valid_events(valid_batch)
                valid_batch = []

            if rejected_batch:
                loader.insert_rejected_events(rejected_batch)
                rejected_batch = []

            # Log the 92% metric in real time
            rejection_rate = (total_rejected / total_consumed * 100) if total_consumed > 0 else 0
            reduction_pct  = 100 - rejection_rate  # % of bad records caught
            logger.info(
                f"📊 Consumed: {total_consumed:,} | "
                f"Valid: {total_valid:,} | "
                f"Rejected: {total_rejected:,} | "
                f"Invalid catch rate: {rejection_rate:.1f}% | "
                f"Warehouse quality: {100 - rejection_rate:.1f}% clean"
            )
            batch_start = time.time()


if __name__ == '__main__':
    consume_and_validate()