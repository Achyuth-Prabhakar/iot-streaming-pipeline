""""
IoT Event Producer
------------------
Simulates a factory floor with 20 machines, each emitting sensor events.
Based on statistical distributions from the AI4I 2020 Predictive Maintenance dataset.

Target throughput: 6 records/sec = 518,400 records/day (~500K/day as per resume)
Error injection rate: ~8% of records → validates the 92% reduction metric downstream
"""

import json
import time
import random
import uuid
import os
from datetime import datetime
import numpy as np
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PRODUCER] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# ── Config from environment ──────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC              = os.getenv('KAFKA_TOPIC', 'iot-sensor-events')
RECORDS_PER_SECOND       = float(os.getenv('RECORDS_PER_SECOND', '6'))
ERROR_INJECTION_RATE     = float(os.getenv('ERROR_INJECTION_RATE', '0.08'))  # 8% bad records

# ── Factory simulation config ────────────────────────────────────────────────
MACHINE_IDS    = [f"MACHINE_{str(i).zfill(3)}" for i in range(1, 21)]  # 20 machines
MACHINE_TYPES  = ['L', 'M', 'H']  # Low / Medium / High quality variants

# Statistical distributions from AI4I 2020 dataset
SENSOR_DISTRIBUTIONS = {
    'air_temperature_k':     {'mean': 300.0, 'std': 2.0},
    'process_temperature_k': {'mean': 310.0, 'std': 1.5},
    'rotational_speed_rpm':  {'mean': 1538.0, 'std': 179.0},
    'torque_nm':             {'mean': 40.0,  'std': 10.0},
}

# Failure rates from original dataset (~3.4% total failure rate)
FAILURE_TYPES = {
    'TWF': 0.007,   # Tool Wear Failure
    'HDF': 0.011,   # Heat Dissipation Failure
    'PWF': 0.005,   # Power Failure
    'OSF': 0.003,   # Overstrain Failure
    'RNF': 0.001,   # Random Failure
    'NONE': 0.973,
}

# ── Error types to inject ────────────────────────────────────────────────────
ERROR_TYPES = ['null_field', 'out_of_range', 'invalid_type', 'bad_timestamp', 'schema_mismatch']


def generate_clean_event() -> dict:
    """Generate a valid IoT sensor event from realistic distributions."""
    machine_id   = random.choice(MACHINE_IDS)
    machine_type = random.choice(MACHINE_TYPES)

    air_temp      = np.random.normal(300.0, 2.0)
    process_temp  = air_temp + np.random.normal(10.0, 1.0)   # always above air temp
    rot_speed     = max(100.0, np.random.normal(1538.0, 179.0))
    torque        = max(0.1,   np.random.normal(40.0, 10.0))
    tool_wear     = random.randint(0, 250)
    power         = abs(torque * rot_speed * 0.10472)         # P = T × ω

    # Determine failure (weighted random)
    failure_type    = random.choices(
        list(FAILURE_TYPES.keys()),
        weights=list(FAILURE_TYPES.values()),
        k=1
    )[0]
    machine_failure = 0 if failure_type == 'NONE' else 1

    return {
        'event_id':               str(uuid.uuid4()),
        'machine_id':             machine_id,
        'machine_type':           machine_type,
        'timestamp':              datetime.utcnow().isoformat() + 'Z',
        'air_temperature_k':      round(air_temp, 2),
        'process_temperature_k':  round(process_temp, 2),
        'rotational_speed_rpm':   round(rot_speed, 2),
        'torque_nm':              round(torque, 2),
        'tool_wear_min':          tool_wear,
        'power_w':                round(power, 2),
        'machine_failure':        machine_failure,
        'failure_type':           failure_type,
        'schema_version':         '1.0',
        'ingestion_ts':           datetime.utcnow().isoformat() + 'Z',
    }


def inject_error(event: dict) -> dict:
    """
    Corrupt a valid event in one of 5 ways.
    These are the exact errors the validator will catch downstream.
    """
    error_type = random.choice(ERROR_TYPES)

    if error_type == 'null_field':
        # Null out a critical sensor reading
        field = random.choice(['air_temperature_k', 'rotational_speed_rpm', 'torque_nm'])
        event[field] = None

    elif error_type == 'out_of_range':
        # Sensor value physically impossible
        field = random.choice(['rotational_speed_rpm', 'torque_nm', 'air_temperature_k'])
        event[field] = random.choice([-9999, 0, 99999])

    elif error_type == 'invalid_type':
        # String where number expected (firmware bug / encoding error)
        event['tool_wear_min'] = 'SENSOR_ERROR_NaN'

    elif error_type == 'bad_timestamp':
        # Malformed / missing timestamp
        event['timestamp'] = random.choice(['', 'NULL', '0000-00-00', 'not-a-date'])

    elif error_type == 'schema_mismatch':
        # Missing required field entirely
        field = random.choice(['machine_id', 'machine_type', 'event_id'])
        del event[field]

    event['_error_injected'] = error_type  # tag for validation analysis
    return event


def create_kafka_producer(max_retries: int = 15) -> KafkaProducer:
    """Create Kafka producer with retry logic (Kafka takes ~30s to be ready)."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',               # wait for broker acknowledgment
                retries=3,
                batch_size=16384,         # 16KB batch
                linger_ms=5,              # wait 5ms to fill batch
                compression_type='gzip',  # compress before sending
            )
            logger.info(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready. Attempt {attempt}/{max_retries}. Waiting 10s...")
            time.sleep(10)

    raise RuntimeError("❌ Could not connect to Kafka after max retries")


def main():
    producer  = create_kafka_producer()
    interval  = 1.0 / RECORDS_PER_SECOND  # seconds between records

    total_sent     = 0
    errors_sent    = 0
    start_time     = time.time()
    last_log_time  = start_time

    logger.info(
        f"🚀 Starting IoT Producer | "
        f"Rate: {RECORDS_PER_SECOND}/sec | "
        f"Daily target: {RECORDS_PER_SECOND * 86400:,.0f} records | "
        f"Error rate: {ERROR_INJECTION_RATE*100:.0f}%"
    )

    while True:
        loop_start = time.time()

        # Generate event, inject error for ~8% of records
        event       = generate_clean_event()
        is_error    = random.random() < ERROR_INJECTION_RATE
        if is_error:
            event    = inject_error(event)
            errors_sent += 1

        # Machine ID as partition key (all events from same machine go to same partition)
        partition_key = event.get('machine_id', 'UNKNOWN')

        producer.send(
            KAFKA_TOPIC,
            key=partition_key,
            value=event
        )
        total_sent += 1

        # Log every 5 seconds
        now = time.time()
        if now - last_log_time >= 5.0:
            elapsed     = now - start_time
            rate        = total_sent / elapsed
            daily_proj  = rate * 86400
            error_pct   = (errors_sent / total_sent * 100) if total_sent > 0 else 0

            logger.info(
                f"📊 Sent: {total_sent:,} | "
                f"Rate: {rate:.1f}/sec | "
                f"Daily projection: {daily_proj:,.0f} | "
                f"Errors injected: {errors_sent:,} ({error_pct:.1f}%)"
            )
            last_log_time = now

        # Maintain target rate
        elapsed_loop = time.time() - loop_start
        sleep_time   = interval - elapsed_loop
        if sleep_time > 0:
            time.sleep(sleep_time)


if __name__ == '__main__':
    main()