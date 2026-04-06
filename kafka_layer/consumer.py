"""
Kafka Consumer Utilities
"""

import json
import logging
import os
import sys
from typing import Any, Dict, Generator, List, Optional

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ALERTS,
    TOPIC_PROCESSED,
    TOPIC_RAW,
)

log = logging.getLogger(__name__)


class SmartCityConsumer:
    """
    Thin wrapper around KafkaConsumer for the Smart City pipeline.

    Parameters:
        topics : list[str]
            Kafka topics to subscribe to.
        group_id : str
            Consumer group ID.  Use distinct IDs for independent consumers
            (e.g. dashboard vs. archiver).
        auto_offset_reset : str
            ``"latest"`` for live-only, ``"earliest"`` for reprocessing.
        max_poll_records : int
            Max records returned per poll call (tune for latency vs throughput).
    """

    def __init__(
        self,
        topics: List[str],
        group_id: str = "smart-city-default",
        auto_offset_reset: str = "latest",
        max_poll_records: int = 500,
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        cfg: Dict[str, Any] = dict(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            auto_commit_interval_ms=5_000,
            max_poll_records=max_poll_records,
            value_deserializer=self._deserialize,
            consumer_timeout_ms=1_000,   # raise StopIteration after 1s idle
        )
        if extra_config:
            cfg.update(extra_config)

        self._consumer = KafkaConsumer(*topics, **cfg)
        log.info("Consumer subscribed to %s (group=%s)", topics, group_id)


    # Deserialiser

    @staticmethod
    def _deserialize(raw: bytes) -> Optional[Dict[str, Any]]:
        """Decode UTF-8 JSON; return None on parse errors."""
        try:
            return json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.warning("Deserialisation error: %s", exc)
            return None


    # Iterator helpers

    def poll_forever(
        self,
        timeout_ms: int = 1_000,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Yield deserialized event dicts indefinitely.
        Skips ``None`` values produced by failed deserialisation.
        """
        while True:
            records = self._consumer.poll(timeout_ms=timeout_ms)
            for tp, messages in records.items():
                for msg in messages:
                    if msg.value is not None:
                        yield msg.value

    def poll_once(
        self,
        timeout_ms: int = 5_000,
        max_records: int = 1_000,
    ) -> List[Dict[str, Any]]:
        """
        Return up to ``max_records`` events from a single poll call.
        Useful for batch-mode consumers (e.g. DLT micro-batches).
        """
        records = self._consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        events: List[Dict[str, Any]] = []
        for _, messages in records.items():
            for msg in messages:
                if msg.value is not None:
                    events.append(msg.value)
        return events


    # Offset management

    def seek_to_beginning(self) -> None:
        """Rewind all assigned partitions to their earliest offset."""
        self._consumer.seek_to_beginning()

    def seek_to_end(self) -> None:
        """Fast-forward all assigned partitions to the latest offset."""
        self._consumer.seek_to_end()


    # Lifecycle

    def close(self) -> None:
        self._consumer.close()
        log.info("Consumer closed.")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# Pre-configured consumer factories

def raw_consumer(group_id: str = "raw-consumer") -> SmartCityConsumer:
    """Consumer for the iot_sensor_raw topic."""
    return SmartCityConsumer([TOPIC_RAW], group_id=group_id, auto_offset_reset="latest")


def processed_consumer(group_id: str = "processed-consumer") -> SmartCityConsumer:
    """Consumer for the iot_sensor_processed topic."""
    return SmartCityConsumer([TOPIC_PROCESSED], group_id=group_id)


def alerts_consumer(group_id: str = "alerts-consumer") -> SmartCityConsumer:
    """Consumer for the iot_alerts topic."""
    return SmartCityConsumer([TOPIC_ALERTS], group_id=group_id, auto_offset_reset="earliest")


def dashboard_consumer() -> SmartCityConsumer:
    """Multi-topic consumer for the real-time dashboard."""
    return SmartCityConsumer(
        [TOPIC_RAW, TOPIC_PROCESSED, TOPIC_ALERTS],
        group_id="dashboard-consumer",
        auto_offset_reset="latest",
        max_poll_records=2_000,
    )