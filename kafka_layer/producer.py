"""
Kafka Producer Utilities
"""

import json
import logging
import os
import sys
from typing import Any, Callable, Dict, List, Optional

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_NUM_PARTITIONS,
    KAFKA_REPLICATION,
    TOPIC_ALERTS,
    TOPIC_PROCESSED,
    TOPIC_RAW,
)

log = logging.getLogger(__name__)


# Topic management

def ensure_topics(topics: Optional[List[str]] = None) -> None:
    """
    Create Kafka topics if they don't already exist.
    Idempotent — safe to call on every startup.
    """
    topics = topics or [TOPIC_RAW, TOPIC_PROCESSED, TOPIC_ALERTS]
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    new_topics = [
        NewTopic(
            name=t,
            num_partitions=KAFKA_NUM_PARTITIONS,
            replication_factor=KAFKA_REPLICATION,
        )
        for t in topics
    ]
    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        log.info("Topics created: %s", topics)
    except TopicAlreadyExistsError:
        log.debug("Topics already exist — skipping creation.")
    finally:
        admin.close()


# Producer factory

def make_json_producer(
    extra_config: Optional[Dict[str, Any]] = None,
) -> KafkaProducer:
    """
    Build a KafkaProducer that serialises values as UTF-8 JSON and keys as
    plain UTF-8 strings.

    Parameters
    ----------
    extra_config : dict, optional
        Additional kwargs forwarded to KafkaProducer (e.g. acks, compression).
    """
    cfg: Dict[str, Any] = dict(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        compression_type="gzip",
        acks=1,
        linger_ms=5,
        batch_size=65_536,
        max_block_ms=30_000,
    )
    if extra_config:
        cfg.update(extra_config)
    return KafkaProducer(**cfg)


# Typed send helpers

class SmartCityProducer:
    """
    Convenience wrapper that routes events to the correct topic and handles
    key extraction, error logging, and optional on_delivery callbacks.
    """

    def __init__(
        self,
        on_success: Optional[Callable] = None,
        on_error:   Optional[Callable] = None,
    ):
        ensure_topics()
        self._producer = make_json_producer()
        self._on_success = on_success or (lambda meta: None)
        self._on_error   = on_error   or (lambda exc, msg: log.error(
            "Delivery failed for %s: %s", msg, exc
        ))

    def send_raw(self, event: Dict[str, Any]) -> None:
        """Publish a raw IoT event, keyed by sensor_id."""
        self._send(TOPIC_RAW, key=event.get("sensor_id"), value=event)

    def send_processed(self, event: Dict[str, Any]) -> None:
        """Publish a validated/cleaned event."""
        self._send(TOPIC_PROCESSED, key=event.get("sensor_id"), value=event)

    def send_alert(self, alert: Dict[str, Any]) -> None:
        """Publish an alert record."""
        self._send(TOPIC_ALERTS, key=alert.get("sensor_id"), value=alert)

    def send_batch(self, topic: str, events: List[Dict[str, Any]], key_field: str = "sensor_id") -> int:
        """
        Publish a list of events to ``topic``.
        Returns the count of successfully enqueued messages.
        """
        sent = 0
        for event in events:
            try:
                self._send(topic, key=event.get(key_field), value=event)
                sent += 1
            except KafkaError as exc:
                self._on_error(exc, event)
        self._producer.flush()
        return sent

    def _send(self, topic: str, key: Optional[str], value: Dict[str, Any]) -> None:
        future = self._producer.send(topic, key=key, value=value)
        future.add_callback(self._on_success)
        future.add_errback(lambda exc: self._on_error(exc, value))

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()