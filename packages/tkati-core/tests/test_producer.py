import time

import orjson
import pyarrow as pa
from confluent_kafka import Consumer
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
    messages = []  # type: ignore
    deadline = time.time() + timeout
    while len(messages) < count and time.time() < deadline:
        batch = consumer.consume(num_messages=count - len(messages), timeout=1.0)
        messages.extend(m for m in batch if not m.error())
    return messages


def test_from_output_settings_sets_attributes(output_settings: KafkaOutputSettings):
    producer = KafkaProducer.from_output_settings(output_settings)
    try:
        assert producer.topic_name == output_settings.topic.name
        assert producer.format == output_settings.topic.format
        assert producer.key_column == output_settings.topic.key_column
    finally:
        producer.close()


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
