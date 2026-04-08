"""
Tests loader.py inserts against real Snowflake.
Run from consumer/ folder with .env loaded.
"""
import sys, os
sys.path.insert(0, '.')
from dotenv import load_dotenv
load_dotenv(dotenv_path='../.env')

from loader import SnowflakeLoader

loader = SnowflakeLoader()
loader.ensure_tables_exist()

# Test 1 — insert one valid event
valid = [{
    'event_id':               'test-loader-001',
    'machine_id':             'MACHINE_001',
    'machine_type':           'M',
    'timestamp':              '2026-04-02T21:00:00Z',
    'air_temperature_k':      300.0,
    'process_temperature_k':  310.0,
    'rotational_speed_rpm':   1500.0,
    'torque_nm':              40.0,
    'tool_wear_min':          100,
    'power_w':                6283.0,
    'machine_failure':        0,
    'failure_type':           'NONE',
    'schema_version':         '1.0',
}]
loader.insert_valid_events(valid, batch_id='test-batch-001')
print("✅ Valid event inserted")

# Test 2 — insert one rejected event
import json
rejected = [{
    'event_id':        'test-bad-001',
    'machine_id':      'UNKNOWN',
    'raw_payload':     json.dumps({'broken': True, 'field': None}),
    'error_code':      'NULL_VALUE',
    'error_detail':    'air_temperature_k is null',
    'rejected_at':     '2026-04-02T21:00:01Z',
    'kafka_partition': 0,
    'kafka_offset':    99,
}]
loader.insert_rejected_events(rejected, batch_id='test-batch-001')
print("✅ Rejected event inserted")

# Test 3 — log metrics
loader.log_pipeline_metrics(
    batch_id='test-batch-001',
    total_consumed=50,
    total_valid=46,
    total_rejected=4,
    records_per_sec=6.0
)
print("✅ Metrics logged")

loader.close()
print("\n✅ All loader tests passed — check Snowflake tables to confirm rows")
