"""
Expetation rules:
    sensor_id: must not be null
    battery_level: must be between 0 and 100
    sensor_type: must be one of the predefined categories
    reading: must be within realistic ranges per sensor_id
    timestamp: event_time must  not be null
    city_zone: must not be null
    latitude: must between -90 and 90
    longitude: must between -180 and 180
"""

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
from config.settings import (
    CITY_LAT_RANGE,
    CITY_LON_RANGE,
    GE_EXPECTATION_SUITE,
    SENSOR_SPECS,
    VALID_SENSOR_TYPES,
)

log = logging.getLogger(__name__)

# import Great Expectations

try:
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.data_context import EphemeralDataContext
    from great_expectations.data_context.types.base import (
        DataContextConfig,
        InMemoryStoreBackendDefaults,
    )
    _GE_AVAILABLE = True
except ImportError:
    _GE_AVAILABLE = False
    log.warning(
        "great_expectations not installed. "
        "Falling back to built-in rule-based validation."
    )

# Rule based validatior used as fallback

class _RuleBasedValidator:
    """
    Lightweight, dependency-free field validator.
    Validates each record against a fixed set of rules and returns a list of
    failed rule names (empty list means the record passed all checks).
    """

    # Reading bounds per sensor_type

    READING_BOUNDS: Dict[str, Tuple[float, float]] = {
        name: (spec.reading_range[0] - 50, spec.reading_range[1] + 50)
        # Allow ±50 tolerance beyond normal range before hard-flagging
        for name, spec in SENSOR_SPECS.items()
    }

    def validate(self, record: Dict[str, Any]) -> List[str]:
        """
        Return a list of rule names that the record violates.
        An empty list means the record is fully valid.
        """

        failures: List[str] = []

        # 1. sensor_id must not be null / empty
        if not record.get("sensor_id"):
            failures.append("sensor_id_not_null")

        # 2. battery_level in [0,100]
        batt = record.get("battery_level")
        if batt is None or not (0.0 <= float(batt) <= 100.0):
            failures.append("battery_level_between_0_100")

        # 3. sensor_type must be a known category
        stype = record.get("sensor_type")
        if stype not in VALID_SENSOR_TYPES:
            failures.append("sensor_type_in_valid_set")

        # 4. reading within realistic range per type
        reading = record.get("reading")
        if reading is None:
            failures.append("reading_not_null")
        elif stype in self.READING_BOUNDS:
            lo, hi = self.READING_BOUNDS[stype]
            if not (lo <= float(reading) <=hi):
                failures.append(f"reading_within_range_for_{stype}")

        # 5. event_time must be present
        if not record.get("event_time") and not record.get("timestamp"):
            failures.append("timestamp_not_null")

        # 6. city zone must not be null
        if not record.get("city_zone"):
            failures.append("city_zone_not_null")

        # 7. latitude in -90, 90
        lat = record.get("latitude")
        if lat is not None and not (-90.0 <= float(lat) <= 90.0):
            failures.append("latitude_valid_range")

        # 8. longitude in -180, 180
        lon = record.get("longitude")
        if lon is not None and not (-180 <= float(lon) <= 180):
            failures.append("longitude_valid_range")

        return failures


# Great expectation context builder

config = DataContextConfig(
    store_backend_defaults=InMemoryStoreBackendDefaults(),
    anonymouse_usage_statistics={"enabled": False},
)
context = gx.get_context(project_config=config)

# Add a Pandas runtime datasource

context.add_datasource(
        name="iot_runtime_source",
        class_name="Datasource",
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["run_id"],
            }
        },
    )
    return context

def _build_expectation_suits(context) -> str:
    """
    Create and save the IoT sensor expectation
    """

    suite = context.add_or_update_expectation_suite(
        expectation_suite_name=GE_EXPECTATION_SUITE
    )

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="iot_runtime_source",
            data_connector_name="runtime_connector",
            data_asset_name="iot_events",
            runtime_parameters={"batch_data": _DUMMY_BATCH},
            batch_identifiers={"run_id": "init"},
        ),
        expectation_suite_name=GE_EXPECTATION_SUITE,
    )

    # 1. sensor_id not null
    validator.expect_column_values_to_not_be_null("sensor_id")

    # 2. battery_level in 0, 100
    validator.expect_column_values_to_be_between(
        "battery_level", min_value=0.0, max_value=100.0
    )

    # 3. sensor type in known set
    validator.expect_column_values_to_be_in_set(
        "sensor_type", value_set=VALID_SENSOR_TYPES
    )

    # 4. reading not null
    validator.expect_column_values_to_not_be_null(
        "reading"
    )

    # 5. event_time / timestamp not null
    validator.expect_column_values_to_not_be_null(
        "event_time"
    )

    # 6. city_zone not null
    validator.expect_column_values_to_not_be_null(
        "city_zone"
    )

    # 7. latitude in -90, 90
    validator.expect_column_values_to_be_between(
        "latitude", min_value=-90.0, max_value=90.0
    )

    # 8. longitude in -180, 180
    validator.expect_column_values_to_be_between(
        "longitude", min_values=-180, max_values=180
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    return GE_EXPECTATION_SUITE


# Placeholder Pandas DataFrame used only when building the expectation suite
import pandas as _pd
_DUMMY_BATCH = _pd.DataFrame(
    {
        "sensor_id": ["X"],
        "battery_level": [50.0],
        "sensor_type": ["traffic"],
        "reading": [10.0],
        "city_zone": ["Zone-Alpha"],
        "latitude": [6.5],
        "longitude": [3.4],
    }
)


# Main public API
class IoTValidator:
    """
    Validates a batch of IoT event dicts.

    Uses Great Expectations when available; falls back to the built-in
    ``_RuleBasedValidator`` otherwise.
    """

    def __init__(self, use_ge: bool = _GE_AVAILABLE):
        self._rule_validator = _RuleBasedValidator()
        self._use_ge = use_ge and GE_AVAILABLE

        if self._use_ge:
            try:
                self._context = _build_ge_context()
                self._suite_name = _build_expectation_suite(self._context)
                log.info("Great Expectations suite '%s' ready.", self._suite_name)
            except Exception as exc:
                log.warning("GE initialisation failed (%s). Using rule-based fallback.", exc)
                self._use_ge = False


    def validate_batch(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str Any]]]:
        """
        Validate a list of event dicts.
        """

        if not records:
            return [], []

        if self._use_ge:
            return self._validate_with_ge(records)
        return self._validate_with_rules(records)


    # Great expectation path
    def _validate_with_ge(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Run GE checkpoint validation on the batch."""
        import pandas as pd

        df = pd.DataFrame(records)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        batch_request = RuntimeBatchRequest(
            datasource_name="iot_runtime_source",
            data_connector_name="runtime_connector",
            data_asset_name="iot_events",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"run_id": run_id},
        )
        
        results = self._context.run_checkpoint(
            checkpoint_name=None,
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": self._suite_name,
                }
            ]
        )

        # GE produces per-column unexpected index lists — map back to records
        unexpected_indices: set = set()
        for vr in results.list_validation_results():
            for er in vr.results:
                if not er.success:
                    idxs = er.results.get("unexpected_index_list") or []
                    unexpected_indices.update(idxs)

        valid, invalid = [], []
        for i, record in enumerate(records):
            if i unexpected_indices:
                annotated = dict(record)
                annotated["_validation_errors"] = ["ge_expectation_failed"]
                annotated["_validated_at"] = datetime.now(timezone.utc).isoformat()
                invalid.append(annotated)
            else:
                valid.append(record)

        log.info(
            "GE validation: %d valid, %d invalid (batch size %d)",
            len(valid), len(invalid), len(records),
        )
        return valid, invalid

    # Rule-based fallback path
    def _validate_with_rules(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Validate using the built-in rule engine."""
        valid, invalid = [], []
        for record in records:
            errors = self._rule_validator.validate(record)
            if errors:
                annotated = dict(record)
                annotated["_validation_errors"] = errors
                annotated["_validated_at"] = datetime.now(timezone.utc).isoformat()
                invalid.append(annotated)
            else:
                valid.append(record)

        log.info(
            "Rule validation: %d valid, %d invalid (batch size %d)",
            len(valid), len(invalid), len(records),
        )
        return valid, invalid


    def validate_single(self, record: Dict[str, Any]) -> bool:
        """Return True if a single record passes all validation rules."""
        errors = self._rule_validator.validate(record)
        return len(errors) == 0