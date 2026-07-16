import orjson
import pyarrow as pa
import pytest
from confluent_kafka import Producer
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_core.kafka.settings import KafkaInputSettings


def test_from_input_settings_sets_attributes(input_settings: KafkaInputSettings):
    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        assert consumer.topic_name == input_settings.topic.name
        assert consumer.input_schema == input_settings.topic.schema
    finally:
        consumer.close()


def test_from_input_settings_builds_arrow_schemas(input_settings: KafkaInputSettings):
    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        assert consumer.parse_schema.field("id").type == pa.string()
        assert consumer.cast_schema.field("value").type == pa.int64()
    finally:
        consumer.close()


def test_read_arrow_returns_none_on_empty_topic(
    input_settings: KafkaInputSettings, kafka_input_topic: str
):
    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        result = consumer.read_arrow(
            timeout=2,
            num_messages=10,
        )
        assert result is None
    finally:
        consumer.close()


def test_read_arrow_reads_json_messages(
    input_settings: KafkaInputSettings,
    kafka_input_topic: str,
    raw_producer: Producer,
):
    events = [
        {"id": "a", "value": 1},
        {"id": "b", "value": 2},
        {"id": "c", "value": 3},
    ]
    for event in events:
        raw_producer.produce(kafka_input_topic, value=orjson.dumps(event))
    raw_producer.flush()

    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        table = consumer.read_arrow(
            timeout=5,
            num_messages=10,
        )
    finally:
        consumer.close()

    assert table is not None
    assert len(table) == 3
    assert table.schema.field("id").type == pa.string()
    assert table.schema.field("value").type == pa.int64()
    assert sorted(table.column("id").to_pylist()) == ["a", "b", "c"]  # type: ignore
    assert sorted(table.column("value").to_pylist()) == [1, 2, 3]  # type: ignore


def test_read_arrow_respects_max_events(
    input_settings: KafkaInputSettings,
    kafka_input_topic: str,
    raw_producer: Producer,
):
    for i in range(10):
        raw_producer.produce(
            kafka_input_topic, value=orjson.dumps({"id": str(i), "value": i})
        )
    raw_producer.flush()

    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        table = consumer.read_arrow(
            timeout=5,
            num_messages=3,
        )
    finally:
        consumer.close()

    assert table is not None
    assert len(table) == 3


def test_read_arrow_timestamp_casting(
    kafka_input_topic: str,
    raw_producer: Producer,
    run_id: str,
):
    """Verify timestamp[ms] fields are parsed as int64 and cast to pa.timestamp('ms')."""
    from tkati_core.kafka.settings import (
        KafkaConnectionSettings,
        KafkaConsumerSettings,
        KafkaInputSettings,
        KafkaTopicSettings,
    )

    settings = KafkaInputSettings(
        connection=KafkaConnectionSettings(broker="localhost:9092"),
        topic=KafkaTopicSettings(
            name=kafka_input_topic,
            schema={"ts": "timestamp[ms]"},
        ),
        consumer=KafkaConsumerSettings(
            group_id=f"test-ts-{run_id}",
            auto_offset_reset="earliest",
        ),
    )

    raw_producer.produce(
        kafka_input_topic, value=orjson.dumps({"ts": 1_700_000_000_000})
    )
    raw_producer.flush()

    consumer = KafkaConsumer.from_input_settings(settings)
    try:
        table = consumer.read_arrow(timeout=5, num_messages=1)
    finally:
        consumer.close()

    assert table is not None
    assert table.schema.field("ts").type == pa.timestamp("ms")
    assert table.column("ts")[0].as_py().timestamp() * 1000 == pytest.approx(
        1_700_000_000_000
    )


def test_read_pylist_returns_none_on_empty_topic(
    input_settings: KafkaInputSettings, kafka_input_topic: str
):
    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        result = consumer.read_pylist(
            timeout=2,
            num_messages=10,
        )
        assert result is None
    finally:
        consumer.close()


def test_read_pylist_reads_json_messages(
    input_settings: KafkaInputSettings,
    kafka_input_topic: str,
    raw_producer: Producer,
):
    events = [
        {"id": "a", "value": 1},
        {"id": "b", "value": 2},
        {"id": "c", "value": 3},
    ]
    for event in events:
        raw_producer.produce(kafka_input_topic, value=orjson.dumps(event))
    raw_producer.flush()

    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        rows = consumer.read_pylist(
            timeout=5,
            num_messages=10,
        )
    finally:
        consumer.close()

    assert rows is not None
    assert len(rows) == 3
    assert sorted(r["id"] for r in rows) == ["a", "b", "c"]
    assert sorted(r["value"] for r in rows) == [1, 2, 3]
