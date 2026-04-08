"""
Smoke test — reads 50 events from Kafka, runs validator, prints results.
No Snowflake needed. Run while Docker is up.
"""
import json
import os
from kafka import KafkaConsumer
from validator import validate_event

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC             = os.getenv('KAFKA_TOPIC', 'iot-sensor-events')
SAMPLE_SIZE             = 50

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    consumer_timeout_ms=15000,   # stop after 15s of no messages
    group_id='smoke-test-group',
)

print(f"\n🔍 Reading {SAMPLE_SIZE} events from Kafka topic '{KAFKA_TOPIC}'...\n")

valid_count    = 0
rejected_count = 0
error_counts   = {}
count          = 0

for message in consumer:
    if count >= SAMPLE_SIZE:
        break

    event  = message.value
    result = validate_event(event)

    if result.is_valid:
        valid_count += 1
    else:
        rejected_count += 1
        error_counts[result.error_code] = error_counts.get(result.error_code, 0) + 1

    count += 1

consumer.close()

print(f"{'─'*45}")
print(f"  Total sampled  : {count}")
print(f"  Valid          : {valid_count}  ({valid_count/count*100:.1f}%)")
print(f"  Rejected       : {rejected_count}  ({rejected_count/count*100:.1f}%)")
if error_counts:
    print(f"\n  Error breakdown:")
    for code, n in sorted(error_counts.items(), key=lambda x: -x[1]):
        print(f"    {code:<25} {n}")
print(f"{'─'*45}")
print(f"\n✅ Validator is catching ~{rejected_count/count*100:.0f}% of bad records")
print(f"   Target is ~8% (matching the 92% reduction metric)\n")