"""
Snowflake Loader
----------------
Handles all writes to Snowflake:
  - Valid events    → IOT_EVENTS
  - Rejected events → IOT_EVENTS_REJECTED
  - Run metrics     → PIPELINE_METRICS
"""

import os
import uuid
import json
import logging
from datetime import datetime
import snowflake.connector
from snowflake.connector.errors import DatabaseError
from dotenv import load_dotenv

load_dotenv(dotenv_path=r'C:\Users\eshae\repos\iot-streaming-pipeline\.env')

logger = logging.getLogger(__name__)


class SnowflakeLoader:

    def __init__(self):
        self.account   = os.getenv('SNOWFLAKE_ACCOUNT')
        self.user      = os.getenv('SNOWFLAKE_USER')
        self.password  = os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        self.database  = os.getenv('SNOWFLAKE_DATABASE', 'IOT_PIPELINE_DB')
        self.schema    = os.getenv('SNOWFLAKE_SCHEMA', 'RAW')
        self.role      = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        self._conn     = None

    # ── Connection management ─────────────────────────────────────────────────

    def _get_conn(self):
        """Return existing connection or create a new one."""
        if self._conn is None or self._conn.is_closed():
            self._conn = snowflake.connector.connect(
                account   = self.account,
                user      = self.user,
                password  = self.password,
                warehouse = self.warehouse,
                database  = self.database,
                schema    = self.schema,
                role      = self.role,
            )
            logger.info("✅ Snowflake connection established")
        return self._conn

    def close(self):
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            logger.info("Snowflake connection closed")

    # ── Table existence check ─────────────────────────────────────────────────

    def ensure_tables_exist(self):
        """Verify all 3 tables exist — fail fast if schema wasn't run."""
        conn = self._get_conn()
        cur  = conn.cursor()
        try:
            cur.execute(f"SHOW TABLES IN SCHEMA {self.database}.{self.schema}")
            tables   = {row[1].upper() for row in cur.fetchall()}
            required = {'IOT_EVENTS', 'IOT_EVENTS_REJECTED', 'PIPELINE_METRICS'}
            missing  = required - tables
            if missing:
                raise RuntimeError(
                    f"Missing Snowflake tables: {missing}. "
                    f"Run snowflake/schema.sql first."
                )
            logger.info(f"✅ All Snowflake tables verified: {required}")
        finally:
            cur.close()

    # ── Insert valid events ───────────────────────────────────────────────────

    def insert_valid_events(self, events: list, batch_id: str = None) -> int:
        """
        Bulk insert validated IoT events into IOT_EVENTS.
        Returns number of rows inserted.
        """
        if not events:
            return 0

        batch_id = batch_id or str(uuid.uuid4())
        conn     = self._get_conn()
        cur      = conn.cursor()

        sql = """
            INSERT INTO IOT_EVENTS (
                EVENT_ID, MACHINE_ID, MACHINE_TYPE, EVENT_TIMESTAMP,
                AIR_TEMPERATURE_K, PROCESS_TEMPERATURE_K, ROTATIONAL_SPEED_RPM,
                TORQUE_NM, TOOL_WEAR_MIN, POWER_W,
                MACHINE_FAILURE, FAILURE_TYPE, SCHEMA_VERSION,
                INGESTED_AT, PIPELINE_BATCH_ID
            ) VALUES (
                %(event_id)s, %(machine_id)s, %(machine_type)s, %(timestamp)s,
                %(air_temperature_k)s, %(process_temperature_k)s, %(rotational_speed_rpm)s,
                %(torque_nm)s, %(tool_wear_min)s, %(power_w)s,
                %(machine_failure)s, %(failure_type)s, %(schema_version)s,
                CURRENT_TIMESTAMP(), %(batch_id)s
            )
        """

        rows = []
        for e in events:
            rows.append({
                'event_id':               e.get('event_id'),
                'machine_id':             e.get('machine_id'),
                'machine_type':           e.get('machine_type'),
                'timestamp':              e.get('timestamp'),
                'air_temperature_k':      float(e.get('air_temperature_k', 0)),
                'process_temperature_k':  float(e.get('process_temperature_k', 0)),
                'rotational_speed_rpm':   float(e.get('rotational_speed_rpm', 0)),
                'torque_nm':              float(e.get('torque_nm', 0)),
                'tool_wear_min':          int(e.get('tool_wear_min', 0)),
                'power_w':                float(e.get('power_w', 0)) if e.get('power_w') else None,
                'machine_failure':        int(e.get('machine_failure', 0)),
                'failure_type':           e.get('failure_type', 'NONE'),
                'schema_version':         e.get('schema_version', '1.0'),
                'batch_id':               batch_id,
            })

        try:
            cur.executemany(sql, rows)
            conn.commit()
            logger.info(f"✅ Inserted {len(rows)} valid events (batch: {batch_id[:8]}...)")
            return len(rows)
        except DatabaseError as e:
            logger.error(f"❌ Failed to insert valid events: {e}")
            conn.rollback()
            raise
        finally:
            cur.close()

    # ── Insert rejected events ────────────────────────────────────────────────

    def insert_rejected_events(self, rejected: list, batch_id: str = None) -> int:
        """
        Insert rejected records one by one into IOT_EVENTS_REJECTED.
        Uses execute (not executemany) due to PARSE_JSON + TIMESTAMP casting.
        """
        if not rejected:
            return 0

        batch_id = batch_id or str(uuid.uuid4())
        conn     = self._get_conn()
        cur      = conn.cursor()

        sql = """
            INSERT INTO IOT_EVENTS_REJECTED (
                EVENT_ID, MACHINE_ID, RAW_PAYLOAD,
                ERROR_CODE, ERROR_DETAIL, REJECTED_AT,
                KAFKA_PARTITION, KAFKA_OFFSET, PIPELINE_BATCH_ID
            )
            SELECT
                %(event_id)s, %(machine_id)s, PARSE_JSON(%(raw_payload)s),
                %(error_code)s, %(error_detail)s, %(rejected_at)s::TIMESTAMP_TZ,
                %(kafka_partition)s, %(kafka_offset)s, %(batch_id)s
            """

        rows = []
        for r in rejected:
            rows.append({
                'event_id':        r.get('event_id', 'UNKNOWN'),
                'machine_id':      r.get('machine_id', 'UNKNOWN'),
                'raw_payload':     r.get('raw_payload', '{}'),
                'error_code':      r.get('error_code', 'UNKNOWN'),
                'error_detail':    str(r.get('error_detail', ''))[:500],
                'rejected_at':     r.get('rejected_at', datetime.utcnow().isoformat() + 'Z'),
                'kafka_partition': r.get('kafka_partition'),
                'kafka_offset':    r.get('kafka_offset'),
                'batch_id':        batch_id,
            })

        try:
            for row in rows:
                cur.execute(sql, row)
            conn.commit()
            logger.info(f"⚠️  Inserted {len(rows)} rejected events (batch: {batch_id[:8]}...)")
            return len(rows)
        except DatabaseError as e:
            logger.error(f"❌ Failed to insert rejected events: {e}")
            conn.rollback()
            raise
        finally:
            cur.close()

    # ── Log pipeline metrics ──────────────────────────────────────────────────

    def log_pipeline_metrics(
        self,
        batch_id:        str,
        total_consumed:  int,
        total_valid:     int,
        total_rejected:  int,
        records_per_sec: float,
        status:          str = 'SUCCESS'
    ):
        """Write a metrics row — proves the 92% claim over time."""
        conn = self._get_conn()
        cur  = conn.cursor()

        valid_pct    = round(total_valid    / total_consumed * 100, 2) if total_consumed else 0
        rejected_pct = round(total_rejected / total_consumed * 100, 2) if total_consumed else 0

        sql = """
            INSERT INTO PIPELINE_METRICS (
                BATCH_ID, TOTAL_CONSUMED, TOTAL_VALID, TOTAL_REJECTED,
                VALID_PCT, REJECTED_PCT, RECORDS_PER_SECOND, PIPELINE_STATUS
            ) VALUES (
                %(batch_id)s, %(total_consumed)s, %(total_valid)s, %(total_rejected)s,
                %(valid_pct)s, %(rejected_pct)s, %(records_per_sec)s, %(status)s
            )
        """

        try:
            cur.execute(sql, {
                'batch_id':        batch_id,
                'total_consumed':  total_consumed,
                'total_valid':     total_valid,
                'total_rejected':  total_rejected,
                'valid_pct':       valid_pct,
                'rejected_pct':    rejected_pct,
                'records_per_sec': records_per_sec,
                'status':          status,
            })
            conn.commit()
            logger.info(
                f"📊 Metrics logged — "
                f"consumed: {total_consumed}, "
                f"valid: {valid_pct}%, "
                f"rejected: {rejected_pct}%"
            )
        except DatabaseError as e:
            logger.error(f"❌ Failed to log metrics: {e}")
            conn.rollback()
            raise
        finally:
            cur.close()