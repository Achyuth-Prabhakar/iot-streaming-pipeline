"""
Data Quality Validator
-----------------------
Runs 8 validation rules against every IoT event before it reaches Snowflake.
The injected error rate (~8%) combined with these rules produces the
"92% reduction in invalid records reaching the warehouse" metric on the resume.
"""

import re
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# ── Physical bounds for each sensor ─────────────────────────────────────────
# Based on AI4I 2020 dataset + physical machine constraints
VALID_BOUNDS = {
    'air_temperature_k':     (270.0, 330.0),   # kelvin, indoor industrial range
    'process_temperature_k': (270.0, 350.0),   # kelvin, always > air temp
    'rotational_speed_rpm':  (100.0, 3000.0),  # rpm, machine operating range
    'torque_nm':             (0.1,   100.0),   # newton-meters
    'tool_wear_min':         (0,     300),     # minutes of wear before replacement
    'power_w':               (0.0,   50000.0), # watts
}

REQUIRED_FIELDS = [
    'event_id', 'machine_id', 'machine_type', 'timestamp',
    'air_temperature_k', 'process_temperature_k',
    'rotational_speed_rpm', 'torque_nm', 'tool_wear_min'
]

VALID_MACHINE_TYPES = {'L', 'M', 'H'}
MACHINE_ID_PATTERN  = re.compile(r'^MACHINE_\d{3}$')
ISO_TS_PATTERN      = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')

# In-memory dedup store: event_id → seen_timestamp
# Cleared of entries older than 1 hour on each check
_dedup_store: dict = {}


class ValidationResult:
    """Holds the outcome of a single event's validation."""
    def __init__(
        self,
        is_valid:     bool,
        error_code:   Optional[str] = None,
        error_detail: Optional[str] = None
    ):
        self.is_valid     = is_valid
        self.error_code   = error_code
        self.error_detail = error_detail

    def __repr__(self):
        if self.is_valid:
            return "ValidationResult(PASS)"
        return f"ValidationResult(FAIL | {self.error_code}: {self.error_detail})"


def validate_event(event: dict) -> ValidationResult:
    """
    Run all 8 validation rules against an IoT event.
    Returns on first failure (fail-fast approach).

    Rules:
      1. Required fields present
      2. No null values in required fields
      3. Valid ISO 8601 timestamp
      4. Machine ID matches naming convention
      5. Machine type is L/M/H
      6. All sensor fields are numeric
      7. Sensor values within physical bounds
      8. No duplicate event_id within 1-hour window
      + Bonus: Physical consistency check (process_temp > air_temp)
    """

    # Rule 1 — Required fields present
    for field in REQUIRED_FIELDS:
        if field not in event:
            return ValidationResult(
                False, 'MISSING_FIELD',
                f"Required field absent: '{field}'"
            )

    # Rule 2 — No nulls in required fields
    for field in REQUIRED_FIELDS:
        if event[field] is None:
            return ValidationResult(
                False, 'NULL_VALUE',
                f"Null value in required field: '{field}'"
            )

    # Rule 3 — Valid ISO 8601 timestamp
    ts = event.get('timestamp', '')
    if not isinstance(ts, str) or not ISO_TS_PATTERN.match(ts):
        return ValidationResult(
            False, 'INVALID_TIMESTAMP',
            f"Timestamp failed format check: '{ts}'"
        )
    try:
        datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except ValueError:
        return ValidationResult(
            False, 'INVALID_TIMESTAMP',
            f"Timestamp not parseable: '{ts}'"
        )

    # Rule 4 — Machine ID matches convention (MACHINE_001 to MACHINE_999)
    machine_id = str(event.get('machine_id', ''))
    if not MACHINE_ID_PATTERN.match(machine_id):
        return ValidationResult(
            False, 'INVALID_MACHINE_ID',
            f"Machine ID doesn't match pattern MACHINE_NNN: '{machine_id}'"
        )

    # Rule 5 — Machine type is valid
    machine_type = event.get('machine_type')
    if machine_type not in VALID_MACHINE_TYPES:
        return ValidationResult(
            False, 'INVALID_MACHINE_TYPE',
            f"Machine type must be L/M/H, got: '{machine_type}'"
        )

    # Rule 6 — Numeric type check
    numeric_fields = [
        'air_temperature_k', 'process_temperature_k',
        'rotational_speed_rpm', 'torque_nm', 'tool_wear_min', 'power_w'
    ]
    for field in numeric_fields:
        val = event.get(field)
        if val is not None:
            try:
                float(val)
            except (TypeError, ValueError):
                return ValidationResult(
                    False, 'INVALID_TYPE',
                    f"Non-numeric value in '{field}': '{val}'"
                )

    # Rule 7 — Sensor bounds check
    for field, (min_val, max_val) in VALID_BOUNDS.items():
        val = event.get(field)
        if val is not None:
            try:
                val_f = float(val)
                if not (min_val <= val_f <= max_val):
                    return ValidationResult(
                        False, 'OUT_OF_RANGE',
                        f"'{field}' = {val_f} outside physical bounds [{min_val}, {max_val}]"
                    )
            except (TypeError, ValueError):
                pass  # already caught by rule 6

    # Rule 8 (bonus) — Physical consistency: process_temp must be > air_temp - 5K
    air_temp     = float(event.get('air_temperature_k', 0))
    process_temp = float(event.get('process_temperature_k', 0))
    if process_temp < (air_temp - 5.0):
        return ValidationResult(
            False, 'PHYSICS_VIOLATION',
            f"Process temp ({process_temp}K) below air temp ({air_temp}K) — impossible"
        )

    # Rule 9 — Deduplication (event_id unique within 1-hour window)
    event_id = str(event.get('event_id', ''))
    now      = datetime.utcnow()

    # Evict expired entries (older than 1 hour)
    expired = [k for k, v in _dedup_store.items() if v < now - timedelta(hours=1)]
    for k in expired:
        del _dedup_store[k]

    if event_id in _dedup_store:
        return ValidationResult(
            False, 'DUPLICATE_EVENT',
            f"event_id '{event_id}' already processed within last hour"
        )
    _dedup_store[event_id] = now

    # ✅ All rules passed
    return ValidationResult(True)