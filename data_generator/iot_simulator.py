"""
Generates 500,000+ synthetic sensor events every 2 minutes (~4,000 events/sec)
and publishes them to the Kafka topic `iot_sensor_raw`.
"""

import json
import logging
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List

from kafka import KafkaProducer
from kafka.errors import KafkaError

# local imports
import sys

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import (
    BATCH_PRODUCE_SIZE,
    CITY_LAT_RANGE,
    CITY_LON_RANGE,
    CITY_ZONES,
    CYCLE_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    SENSOR_SPECS,
    TARGET_EVENTS_PER_CYCLE,
    TOPIC_RAW,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("iot_simulator")


# Sensor pool — pre-built at startup for fast O(1) lookup during event gen

def _build_sensor_pool() -> List[Dict[str, Any]]:
    """Return a list of static sensor metadata dicts."""
    pool: List[Dict[str, Any]] = []
    for spec in SENSOR_SPECS.values():
        for i in range(spec.count):
            pool.append(
                {
                    "sensor_id":   f"{spec.sensor_type.upper()}-{i:05d}",
                    "sensor_type": spec.sensor_type,
                    "city_zone":   random.choice(CITY_ZONES),
                    "latitude":    round(random.uniform(*CITY_LAT_RANGE), 6),
                    "longitude":   round(random.uniform(*CITY_LON_RANGE), 6),
                }
            )
    random.shuffle(pool)
    return pool


SENSOR_POOL: List[Dict[str, Any]] = _build_sensor_pool()
log.info("Sensor pool ready: %d unique sensors", len(SENSOR_POOL))


# Event generation helpers

def _reading_for(sensor_type: str) -> float:
    """Return a realistic sensor reading, with a 2% chance of corruption."""
    spec = SENSOR_SPECS[sensor_type]
    if random.random() < 0.02:
        return round(random.uniform(-999.0, 9_999.0), 2)
    return round(random.uniform(*spec.reading_range), 2)


def generate_event(sensor: Dict[str, Any]) -> Dict[str, Any]:
    """Produce a single IoT sensor event dict."""
    battery = round(random.uniform(0.0, 100.0), 1)
    if random.random() < 0.03:
        battery = round(random.uniform(0.0, 14.9), 1)

    return {
        "event_id":      str(uuid.uuid4()),
        "sensor_id":     sensor["sensor_id"],
        "sensor_type":   sensor["sensor_type"],
        "city_zone":     sensor["city_zone"],
        "latitude":      sensor["latitude"],
        "longitude":     sensor["longitude"],
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "reading":       _reading_for(sensor["sensor_type"]),
        "battery_level": battery,
    }


def generate_batch(batch_size: int) -> List[Dict[str, Any]]:
    """Generate ``batch_size`` events sampled from the sensor pool."""
    sensors = random.choices(SENSOR_POOL, k=batch_size)
    return [generate_event(s) for s in sensors]



# Kafka producer factory

def make_producer() -> KafkaProducer:
    """Build a KafkaProducer with JSON serialisation and gzip compression."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        compression_type="gzip",
        acks=1,
        linger_ms=5,
        batch_size=65_536,
        max_block_ms=30_000,
    )



# Threaded publish worker

def _publish_batch(producer: KafkaProducer, events: List[Dict]) -> int:
    """Publish a list of events to TOPIC_RAW. Returns number sent."""
    sent = 0
    for event in events:
        try:
            producer.send(
                TOPIC_RAW,
                key=event["sensor_id"],
                value=event,
            )
            sent += 1
        except KafkaError as exc:
            log.warning("Kafka send failed: %s", exc)
    producer.flush()
    return sent

def _thread_worker(producer: KafkaProducer, batches: int, batch_size: int) -> int:
    """Worker function executed in each producer thread."""
    worker_sent = 0
    for _ in range(batches):
        batch = generate_batch(batch_size)
        worker_sent += _publish_batch(producer, batch)
    return worker_sent



# Main simulation loop

class IoTSimulator:
    """
    High-throughput IoT event simulator.
    Parameters:
        n_workers : int
            Number of parallel producer threads. Each thread owns its own
            KafkaProducer to avoid lock contention.
    """

    def __init__(self, n_workers: int = 8):
        self.n_workers = n_workers
        self.producers = [make_producer() for _ in range(n_workers)]
        log.info("Initialised %d Kafka producer threads", n_workers)

    def _run_cycle(self) -> int:
        """
        Produce TARGET_EVENTS_PER_CYCLE events within one 2-minute cycle.
        Returns total events published.
        """
        total_sent = 0
        events_per_worker  = TARGET_EVENTS_PER_CYCLE // self.n_workers
        batches_per_worker = max(1, events_per_worker // BATCH_PRODUCE_SIZE)

        with ThreadPoolExecutor(max_workers=self.n_workers) as pool:
            futures = [
                pool.submit(
                    _thread_worker,
                    self.producers[i],
                    batches_per_worker,
                    BATCH_PRODUCE_SIZE,
                )
                for i in range(self.n_workers)
            ]
            for future in as_completed(futures):
                try:
                    total_sent += future.result()
                except Exception as exc:
                    log.error("Worker error: %s", exc)

        return total_sent

    def run_forever(self) -> None:
        """Continuously produce events in 2-minute cycles."""
        cycle_num = 0
        while True:
            cycle_num += 1
            cycle_start = time.monotonic()
            log.info("── Cycle %d start ──────────────────────────────", cycle_num)

            sent = self._run_cycle()

            elapsed   = time.monotonic() - cycle_start
            rate      = sent / elapsed if elapsed > 0 else 0
            sleep_for = max(0.0, CYCLE_SECONDS - elapsed)

            log.info(
                "Cycle %d done: %d events in %.1fs (%.0f ev/s). "
                "Sleeping %.1fs until next cycle.",
                cycle_num, sent, elapsed, rate, sleep_for,
            )
            time.sleep(sleep_for)

    def close(self) -> None:
        for p in self.producers:
            p.close()



# CLI entry-point

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Smart City IoT Simulator")
    parser.add_argument(
        "--workers", type=int, default=8,
        help="Number of parallel Kafka producer threads (default: 8)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single cycle then exit (useful for testing)",
    )
    args = parser.parse_args()

    sim = IoTSimulator(n_workers=args.workers)
    try:
        if args.once:
            sent = sim._run_cycle()
            log.info("Single-cycle mode: %d events sent.", sent)
        else:
            sim.run_forever()
    except KeyboardInterrupt:
        log.info("Simulator stopped by user.")
    finally:
        sim.close()