"""Real Kafka round trip for fetch_kafka_snapshot. Requires a broker on localhost:9092
(see tests/conftest.py's kafka_topic fixture), same as the e2e tests in tkati-node-el/-dedup.
"""

import time

import orjson
import pytest
from confluent_kafka import Producer
from tkati_dashboard.snapshot import SnapshotError, fetch_kafka_snapshot

BROKER = "localhost:9092"


def _produce(topic: str, events: list[dict]) -> None:
    producer = Producer({"bootstrap.servers": BROKER})
    for event in events:
        producer.produce(topic, orjson.dumps(event))
    producer.flush()


def test_fetch_kafka_snapshot_returns_latest_events(kafka_topic: str) -> None:
    events = [{"id": str(i), "amount": i} for i in range(5)]
    _produce(kafka_topic, events)
    time.sleep(0.5)  # let the produced messages land before we compute watermarks

    result = fetch_kafka_snapshot(BROKER, kafka_topic, limit=3)

    assert [e["value"]["id"] for e in result] == ["2", "3", "4"]
    # Kafka-level metadata alongside the parsed value.
    assert [e["offset"] for e in result] == [2, 3, 4]
    assert all(e["partition"] == 0 for e in result)
    assert all(isinstance(e["timestamp"], int) for e in result)
    assert all(e["timestamp_type"] in ("create", "log_append") for e in result)


def test_fetch_kafka_snapshot_empty_topic(kafka_topic: str) -> None:
    assert fetch_kafka_snapshot(BROKER, kafka_topic) == []


def test_fetch_kafka_snapshot_unreachable_broker_raises() -> None:
    # An address nothing listens on, so metadata lookup fails deterministically instead of
    # depending on whether the target broker has topic auto-creation enabled.
    with pytest.raises(SnapshotError):
        fetch_kafka_snapshot("127.0.0.1:1", "some-topic", timeout_sec=1.0)
