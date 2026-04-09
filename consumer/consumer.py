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
    connection_string = os.getenv('KAFKA_CONNECTION_STRING', '')
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

    if connection_string:
        # Azure Event Hubs
        consumer_config = {
            'bootstrap_servers': bootstrap_servers,
            'security_protocol': 'SASL_SSL',
            'sasl_mechanism': 'PLAIN',
            'sasl_plain_username': '$ConnectionString',
            'sasl_plain_password': connection_string,
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'group_id': 'iot-validator-group',
            'max_poll_records': 500,
            'session_timeout_ms': 30000,
            'consumer_timeout_ms': BATCH_TIMEOUT_SECS * 1000,
        }
    else:
        # Local Kafka
        consumer_config = {
            'bootstrap_servers': bootstrap_servers.split(','),
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            'group_id': 'iot-validator-group',
            'max_poll_records': 500,
            'session_timeout_ms': 30000,
        }

    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(**consumer_config)
            logger.info(f"✅ Consumer connected to {bootstrap_servers}")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready. Attempt {attempt}/{max_retries}. Waiting 10s...")
            time.sleep(10)
    raise RuntimeError("Could not connect to Kafka")


if __name__ == '__main__':
    consume_and_validate()