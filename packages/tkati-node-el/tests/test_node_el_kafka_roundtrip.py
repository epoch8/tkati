import time

import orjson
from confluent_kafka import Consumer as RawConsumer
from confluent_kafka import Producer as RawProducer
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_core.kafka.producer import KafkaProducer
from tkati_core.kafka.settings import KafkaOutputSettings
from tkati_node_el.main import run_one_iteration
from tkati_node_el.settings import AppSettings


def _make_consumer(settings: AppSettings) -> KafkaConsumer:
    return KafkaConsumer(
        kafka_config={
            "bootstrap.servers": settings.input.connection.broker,
            "group.id": settings.input.consumer.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        },
        topic_name=settings.input.topic.name,
        input_schema=settings.input.topic.schema,
    )


def _drain_output(
    settings: AppSettings, expected: int, timeout: float = 10.0
) -> list[dict]:
    assert isinstance(settings.output, KafkaOutputSettings)
    consumer = RawConsumer(
        {
            "bootstrap.servers": settings.output.connection.broker,
            "group.id": f"verify-{settings.output.topic.name}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.output.topic.name])
    rows: list[dict] = []
    deadline = time.time() + timeout
    try:
        while len(rows) < expected and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            value = msg.value()
            assert value is not None
            rows.append(orjson.loads(value))
    finally:
        consumer.close()
    return rows


def test_node_el_kafka_json_roundtrip_preserves_types(
    kafka_producer: RawProducer, kafka_test_settings: AppSettings
) -> None:
    """A full Kafka->Kafka noop pass through run_one_iteration must reproduce every field
    byte-for-byte, including timestamp[ms] as the original epoch-ms int (not an ISO string)."""
    event = {
        "s": "hello",
        "time": 1_700_000_000_000,
        "i32": -42,
        "i64": 9_999_999_999,
        "u32": 42,
        "u64": 42,
        "u8": 7,
    }
    kafka_producer.produce(
        kafka_test_settings.input.topic.name, value=orjson.dumps(event)
    )
    kafka_producer.flush()

    assert isinstance(kafka_test_settings.output, KafkaOutputSettings)
    consumer = _make_consumer(kafka_test_settings)
    producer = KafkaProducer.from_output_settings(kafka_test_settings.output)
    try:
        run_one_iteration(consumer, producer, kafka_test_settings)
    finally:
        consumer.close()

    rows = _drain_output(kafka_test_settings, expected=1)
    assert rows == [event]
