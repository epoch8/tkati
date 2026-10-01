import time
from typing import Literal

import orjson
import pyarrow as pa
import pytest
from confluent_kafka import Consumer
from pydantic import ValidationError
from tkati_core import PRODUCER_PHASES, DeliveryError, LoopStats
from tkati_core._native import NativeProducer
from tkati_core.kafka.producer import KafkaProducer
from tkati_core.kafka.settings import (
    KafkaConnectionSettings,
    KafkaOutputSettings,
    KafkaTopicSettings,
)


def _consume_all(
    consumer: Consumer,
    topic: str,
    count: int,
    timeout: float = 10.0,
) -> list:
    """Consume exactly `count` messages from `topic`, returning raw confluent Message objects."""
    consumer.subscribe([topic])
    messages = []
    deadline = time.time() + timeout
    while len(messages) < count and time.time() < deadline:
        batch = consumer.consume(num_messages=count - len(messages), timeout=1.0)
        messages.extend(m for m in batch if not m.error())
    return messages


def _capture_config(monkeypatch: pytest.MonkeyPatch) -> dict[str, str | int | bool]:
    """Replace `NativeProducer` with a stub recording the config it is built
    with, so a test can assert on it without a broker."""
    captured: dict[str, str | int | bool] = {}

    class CapturingNativeProducer:
        def __init__(self, config: dict[str, str | int | bool], topic: str) -> None:
            captured.update(config)

    monkeypatch.setattr(
        "tkati_core.kafka.producer.NativeProducer", CapturingNativeProducer
    )
    return captured


def test_from_output_settings_sets_attributes(
    output_settings: KafkaOutputSettings, monkeypatch: pytest.MonkeyPatch
):
    captured_config = _capture_config(monkeypatch)
    producer = KafkaProducer.from_output_settings(output_settings)
    assert producer.topic_name == output_settings.topic.name
    assert producer.format == output_settings.topic.format
    assert producer.key_column == output_settings.topic.key_column
    assert captured_config == {"bootstrap.servers": output_settings.connection.broker}


def test_output_config_is_merged_into_the_client_config(
    output_settings: KafkaOutputSettings, monkeypatch: pytest.MonkeyPatch
):
    captured_config = _capture_config(monkeypatch)
    output_settings.config = {
        "compression.type": "zstd",
        "linger.ms": 50,
        "enable.idempotence": True,
    }
    KafkaProducer.from_output_settings(output_settings)
    # Equality, not containment: an accidental extra or renamed property fails.
    assert captured_config == {
        "bootstrap.servers": output_settings.connection.broker,
        "compression.type": "zstd",
        "linger.ms": 50,
        "enable.idempotence": True,
    }


def test_output_config_rejects_a_reserved_property():
    with pytest.raises(ValidationError, match=r"is set from `connection\.broker`"):
        KafkaOutputSettings(
            connection=KafkaConnectionSettings(broker="broker:9092"),
            topic=KafkaTopicSettings(name="t"),
            config={"bootstrap.servers": "elsewhere:9092"},
        )


def test_librdkafka_accepts_a_passthrough_of_each_value_type(
    output_settings: KafkaOutputSettings,
):
    """The stub above only checks our own merge. This builds a real client, so
    the three properties are validated by librdkafka, and a str, an int and a
    bool are all shown to survive the native layer's lowering."""
    NativeProducer(
        {
            "bootstrap.servers": output_settings.connection.broker,
            "compression.type": "zstd",
            "linger.ms": 50,
            "enable.idempotence": True,
        },
        "config-passthrough-probe",
    )


def test_librdkafka_rejects_an_unknown_or_invalid_property(
    output_settings: KafkaOutputSettings,
):
    """Why no property-name validation in pydantic: librdkafka does it, at
    client construction, naming the property."""
    broker = output_settings.connection.broker
    with pytest.raises(Exception, match="No such configuration property"):
        NativeProducer({"bootstrap.servers": broker, "compresion.type": "zstd"}, "t")
    with pytest.raises(Exception, match='Invalid value "brotli"'):
        NativeProducer({"bootstrap.servers": broker, "compression.type": "brotli"}, "t")


def test_produce_json_format(
    output_settings: KafkaOutputSettings,
    kafka_output_topic: str,
    raw_consumer: Consumer,
):
    table = pa.table({"name": ["alice", "bob"], "score": [10, 20]})

    producer = KafkaProducer.from_output_settings(output_settings)
    try:
        producer.produce_arrow(table)
        producer.flush()
    finally:
        producer.close()

    messages = _consume_all(raw_consumer, kafka_output_topic, count=2)
    assert len(messages) == 2
    parsed = [orjson.loads(m.value()) for m in messages]
    names = {r["name"] for r in parsed}
    scores = {r["score"] for r in parsed}
    assert names == {"alice", "bob"}
    assert scores == {10, 20}


def test_produce_json_format_preserves_timestamp(
    kafka_output_topic: str,
    raw_consumer: Consumer,
    run_id: str,
):
    """A timestamp[ms] output schema must cast the column back to its raw epoch-ms int
    before JSON serialization, instead of orjson emitting an ISO-8601 string."""
    settings = KafkaOutputSettings(
        connection=KafkaConnectionSettings(broker="localhost:9092"),
        topic=KafkaTopicSettings(
            name=kafka_output_topic, schema={"ts": "timestamp[ms]"}
        ),
    )
    table = pa.table({"ts": pa.array([1_700_000_000_000], type=pa.int64())}).cast(
        pa.schema([pa.field("ts", pa.timestamp("ms"))])
    )

    producer = KafkaProducer.from_output_settings(settings)
    try:
        producer.produce_arrow(table)
        producer.flush()
    finally:
        producer.close()

    messages = _consume_all(raw_consumer, kafka_output_topic, count=1)
    parsed = orjson.loads(messages[0].value())
    assert parsed["ts"] == 1_700_000_000_000


def test_produce_json_format_no_message_key_by_default(
    output_settings: KafkaOutputSettings,
    kafka_output_topic: str,
    raw_consumer: Consumer,
):
    table = pa.table({"x": [1]})

    producer = KafkaProducer.from_output_settings(output_settings)
    try:
        producer.produce_arrow(table)
        producer.flush()
    finally:
        producer.close()

    messages = _consume_all(raw_consumer, kafka_output_topic, count=1)
    assert messages[0].key() is None


def test_produce_json_format_with_key_column(
    kafka_output_topic: str,
    raw_consumer: Consumer,
    run_id: str,
):
    settings = KafkaOutputSettings(
        connection=KafkaConnectionSettings(broker="localhost:9092"),
        topic=KafkaTopicSettings(
            name=kafka_output_topic,
            key_column="user_id",
        ),
    )
    table = pa.table({"user_id": ["u1", "u2"], "value": [100, 200]})

    producer = KafkaProducer.from_output_settings(settings)
    try:
        producer.produce_arrow(table)
        producer.flush()
    finally:
        producer.close()

    messages = _consume_all(raw_consumer, kafka_output_topic, count=2)
    assert len(messages) == 2
    keys = {m.key().decode() for m in messages}
    assert keys == {"u1", "u2"}


def test_produce_arrow_batch_format(
    kafka_output_topic: str,
    raw_consumer: Consumer,
    run_id: str,
):
    settings = KafkaOutputSettings(
        connection=KafkaConnectionSettings(broker="localhost:9092"),
        topic=KafkaTopicSettings(
            name=kafka_output_topic,
            format="arrow-batch",
        ),
    )
    original = pa.table({"id": ["x", "y", "z"], "n": [1, 2, 3]})

    producer = KafkaProducer.from_output_settings(settings)
    try:
        producer.produce_arrow(original)
        producer.flush()
    finally:
        producer.close()

    messages = _consume_all(raw_consumer, kafka_output_topic, count=1)
    assert len(messages) == 1

    reader = pa.ipc.open_stream(messages[0].value())
    recovered = reader.read_all()

    assert recovered.schema == original.schema
    assert recovered.equals(original)


def test_produce_record_batch(
    output_settings: KafkaOutputSettings,
    kafka_output_topic: str,
    raw_consumer: Consumer,
):
    """produce() accepts pa.RecordBatch as well as pa.Table."""
    batch = pa.record_batch({"x": [7, 8]})

    producer = KafkaProducer.from_output_settings(output_settings)
    try:
        producer.produce_arrow(batch)
        producer.flush()
    finally:
        producer.close()

    messages = _consume_all(raw_consumer, kafka_output_topic, count=2)
    assert len(messages) == 2
    values = {orjson.loads(m.value())["x"] for m in messages}
    assert values == {7, 8}


def test_produce_pylist_json_format(
    output_settings: KafkaOutputSettings,
    kafka_output_topic: str,
    raw_consumer: Consumer,
):
    rows = [{"name": "alice", "score": 10}, {"name": "bob", "score": 20}]

    producer = KafkaProducer.from_output_settings(output_settings)
    try:
        producer.produce_pylist(rows)
        producer.flush()
    finally:
        producer.close()

    messages = _consume_all(raw_consumer, kafka_output_topic, count=2)
    assert len(messages) == 2
    parsed = [orjson.loads(m.value()) for m in messages]
    names = {r["name"] for r in parsed}
    scores = {r["score"] for r in parsed}
    assert names == {"alice", "bob"}
    assert scores == {10, 20}


def test_produce_pylist_with_key_column(
    kafka_output_topic: str,
    raw_consumer: Consumer,
    run_id: str,
):
    settings = KafkaOutputSettings(
        connection=KafkaConnectionSettings(broker="localhost:9092"),
        topic=KafkaTopicSettings(
            name=kafka_output_topic,
            key_column="user_id",
        ),
    )
    rows = [{"user_id": "u1", "value": 100}, {"user_id": "u2", "value": 200}]

    producer = KafkaProducer.from_output_settings(settings)
    try:
        producer.produce_pylist(rows)
        producer.flush()
    finally:
        producer.close()

    messages = _consume_all(raw_consumer, kafka_output_topic, count=2)
    assert len(messages) == 2
    keys = {m.key().decode() for m in messages}
    assert keys == {"u1", "u2"}


@pytest.mark.parametrize("format", ["json", "arrow-batch"])
def test_produce_arrow_and_flush_split_their_time_into_producer_phases(
    kafka_output_topic: str,
    raw_consumer: Consumer,
    format: Literal["json", "arrow-batch"],
):
    """Encoding, handing off to librdkafka and waiting for acks are attributed
    separately, so a node's perf report can say which one a slow produce is."""
    settings = KafkaOutputSettings(
        connection=KafkaConnectionSettings(broker="localhost:9092"),
        topic=KafkaTopicSettings(name=kafka_output_topic, format=format),
    )
    table = pa.table({"id": [f"k{i}" for i in range(50)], "n": list(range(50))})

    stats = LoopStats(phases=PRODUCER_PHASES)
    producer = KafkaProducer.from_output_settings(settings)
    try:
        producer.produce_arrow(table, stats=stats)
        assert "producer/deliver" not in stats.phase_sec
        producer.flush(stats=stats)
    finally:
        producer.close()

    assert stats.phase_sec["producer/serialize"] > 0
    assert stats.phase_sec["producer/enqueue"] > 0
    assert stats.phase_sec["producer/deliver"] > 0


def test_produce_pylist_splits_its_time_into_serialize_and_enqueue(
    output_settings: KafkaOutputSettings,
    kafka_output_topic: str,
    raw_consumer: Consumer,
):
    rows = [{"name": "alice", "score": 10}, {"name": "bob", "score": 20}]

    stats = LoopStats(phases=PRODUCER_PHASES)
    producer = KafkaProducer.from_output_settings(output_settings)
    try:
        producer.produce_pylist(rows, stats=stats)
        producer.flush()
    finally:
        producer.close()

    assert stats.phase_sec["producer/serialize"] > 0
    assert stats.phase_sec["producer/enqueue"] > 0
    assert "producer/deliver" not in stats.phase_sec
    assert len(_consume_all(raw_consumer, kafka_output_topic, count=2)) == 2


def test_wait_delivered_is_true_once_a_tags_messages_are_acked(
    output_settings: KafkaOutputSettings, kafka_output_topic: str
):
    producer = KafkaProducer.from_output_settings(output_settings)
    try:
        producer.produce_pylist([{"id": "a"}, {"id": "b"}], tag=7)
        assert producer.wait_delivered(7, timeout=10) is True
        # A tag nothing was produced with counts as delivered.
        assert producer.wait_delivered(8, timeout=0) is True
    finally:
        producer.close()


def test_wait_delivered_raises_when_the_broker_rejects_a_message(
    output_settings: KafkaOutputSettings, kafka_output_topic: str
):
    """flush() returns once nothing is in flight, delivered or not; the
    rejection must still surface, through wait_delivered."""
    producer = KafkaProducer(
        # Let librdkafka send a message larger than the broker accepts, so it
        # fails at the broker (a delivery report) rather than locally.
        kafka_config={
            "bootstrap.servers": output_settings.connection.broker,
            "message.max.bytes": str(20 << 20),
        },
        topic_name=kafka_output_topic,
    )
    try:
        producer.produce_pylist([{"id": "x" * (5 << 20)}], tag=1)
        producer.flush()
        with pytest.raises(DeliveryError):
            producer.wait_delivered(1, timeout=10)
    finally:
        producer.close()
