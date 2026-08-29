"""Real Kafka round trip for fetch_consumer_lag. Requires a broker on localhost:9092
(see tests/conftest.py's kafka_topic fixture), same as the e2e tests in tkati-node-el/-dedup.
"""

import uuid

import orjson
import pytest
from confluent_kafka import Consumer, Producer
from tkati_dashboard.lag import LagError, fetch_consumer_lag

BROKER = "localhost:9092"


def _produce(topic: str, count: int) -> None:
    producer = Producer({"bootstrap.servers": BROKER})
    for i in range(count):
        producer.produce(topic, orjson.dumps({"i": i}))
    producer.flush()


def _consume_and_commit(topic: str, group_id: str, count: int) -> None:
    """Read+commit the first `count` messages on `topic` as `group_id`, then disconnect —
    simulating a consumer that has processed a prefix of the topic and stopped."""
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    read = 0
    while read < count:
        msg = consumer.poll(timeout=5)
        if msg is None or msg.error():
            continue
        read += 1
    consumer.commit(asynchronous=False)
    consumer.close()


def test_fetch_consumer_lag_reports_uncommitted_backlog(kafka_topic: str) -> None:
    group_id = f"tkati-dashboard-lag-test-{uuid.uuid4().hex[:8]}"
    _produce(kafka_topic, count=10)
    _consume_and_commit(kafka_topic, group_id, count=6)

    result = fetch_consumer_lag(BROKER, kafka_topic, group_id)

    assert result["total_lag"] == 4
    assert result["partitions"] == [
        {"partition": 0, "committed_offset": 6, "high_watermark": 10, "lag": 4}
    ]


def test_fetch_consumer_lag_group_never_committed_is_full_backlog(
    kafka_topic: str,
) -> None:
    _produce(kafka_topic, count=7)

    result = fetch_consumer_lag(
        BROKER, kafka_topic, f"never-consumed-{uuid.uuid4().hex[:8]}"
    )

    assert result["total_lag"] == 7
    assert result["partitions"][0]["committed_offset"] is None


def test_fetch_consumer_lag_unreachable_broker_raises() -> None:
    with pytest.raises(LagError):
        fetch_consumer_lag("127.0.0.1:1", "some-topic", "some-group", timeout_sec=1.0)
