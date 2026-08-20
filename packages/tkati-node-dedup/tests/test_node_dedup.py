import time
from unittest.mock import MagicMock

import orjson
import pyarrow as pa
import pytest
from confluent_kafka import Consumer as RawConsumer
from confluent_kafka import Producer as RawProducer
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_core.kafka.producer import KafkaProducer
from tkati_core.kafka.settings import KafkaOutputSettings
from tkati_node_dedup.main import run_one_iteration
from tkati_node_dedup.settings import AppSettings
from tkati_node_dedup.store import BucketedDedupStore


def _make_consumer(test_settings: AppSettings) -> KafkaConsumer:
    assert test_settings.input.type == "kafka"
    return KafkaConsumer(
        kafka_config={
            "bootstrap.servers": test_settings.input.connection.broker,
            "group.id": test_settings.input.consumer.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        },
        topic_name=test_settings.input.topic.name,
        input_schema=test_settings.input.topic.schema,
    )


def _make_producer(test_settings: AppSettings) -> KafkaProducer:
    assert isinstance(test_settings.output, KafkaOutputSettings)
    return KafkaProducer.from_output_settings(test_settings.output)


def _make_store(test_settings: AppSettings) -> BucketedDedupStore:
    return BucketedDedupStore(
        root_dir=test_settings.dedup.store_dir,
        window_hours=test_settings.dedup.window_hours,
        bucket_hours=test_settings.dedup.bucket_hours,
    )


def _drain_output(test_settings: AppSettings, expected: int, timeout: float = 10.0) -> list[dict]:
    assert isinstance(test_settings.output, KafkaOutputSettings)
    consumer = RawConsumer(
        {
            "bootstrap.servers": test_settings.output.connection.broker,
            "group.id": f"verify-{test_settings.output.topic.name}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([test_settings.output.topic.name])
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


def _event(uid: str | None, val: int) -> dict:
    return {"uid": uid, "time": int(time.time() * 1000), "val": val}


def test_basic_in_batch_dedup(
    kafka_producer: RawProducer, test_settings: AppSettings
) -> None:
    """Two messages with the same uid produced before one poll: only one survives."""
    kafka_producer.produce(test_settings.input.topic.name, value=orjson.dumps(_event("dup-1", 1)))
    kafka_producer.produce(test_settings.input.topic.name, value=orjson.dumps(_event("dup-1", 2)))
    kafka_producer.flush()

    consumer = _make_consumer(test_settings)
    producer = _make_producer(test_settings)
    store = _make_store(test_settings)
    try:
        run_one_iteration(consumer, producer, store, test_settings)
    finally:
        consumer.close()
        store.close()

    rows = _drain_output(test_settings, expected=1)
    assert len(rows) == 1
    assert rows[0]["uid"] == "dup-1"


def test_cross_batch_dedup(kafka_producer: RawProducer, test_settings: AppSettings) -> None:
    """Same uid produced across two separate iterations: only the first survives."""
    consumer = _make_consumer(test_settings)
    producer = _make_producer(test_settings)
    store = _make_store(test_settings)
    try:
        kafka_producer.produce(test_settings.input.topic.name, value=orjson.dumps(_event("dup-2", 1)))
        kafka_producer.flush()
        run_one_iteration(consumer, producer, store, test_settings)

        kafka_producer.produce(test_settings.input.topic.name, value=orjson.dumps(_event("dup-2", 2)))
        kafka_producer.flush()
        run_one_iteration(consumer, producer, store, test_settings)
    finally:
        consumer.close()
        store.close()

    rows = _drain_output(test_settings, expected=1)
    assert len(rows) == 1


def test_bucket_rollover_lets_key_through_again(
    kafka_producer: RawProducer, test_settings: AppSettings, monkeypatch
) -> None:
    test_settings.dedup.window_hours = 1
    test_settings.dedup.bucket_hours = 1

    now = [1_000_000.0]
    monkeypatch.setattr("tkati_node_dedup.store._now", lambda: now[0])

    consumer = _make_consumer(test_settings)
    producer = _make_producer(test_settings)
    store = _make_store(test_settings)
    try:
        kafka_producer.produce(test_settings.input.topic.name, value=orjson.dumps(_event("dup-3", 1)))
        kafka_producer.flush()
        run_one_iteration(consumer, producer, store, test_settings)

        # Advance well past window_hours + bucket_hours so the bucket ages out.
        now[0] += 5 * 3600

        kafka_producer.produce(test_settings.input.topic.name, value=orjson.dumps(_event("dup-3", 2)))
        kafka_producer.flush()
        run_one_iteration(consumer, producer, store, test_settings)
    finally:
        consumer.close()
        store.close()

    rows = _drain_output(test_settings, expected=2)
    assert len(rows) == 2


def test_null_dedup_field_passes_through(
    kafka_producer: RawProducer, test_settings: AppSettings
) -> None:
    kafka_producer.produce(test_settings.input.topic.name, value=orjson.dumps(_event(None, 1)))
    kafka_producer.produce(test_settings.input.topic.name, value=orjson.dumps(_event(None, 2)))
    kafka_producer.flush()

    consumer = _make_consumer(test_settings)
    producer = _make_producer(test_settings)
    store = _make_store(test_settings)
    try:
        run_one_iteration(consumer, producer, store, test_settings)
    finally:
        consumer.close()
        store.close()

    rows = _drain_output(test_settings, expected=2)
    assert len(rows) == 2


def test_missing_dedup_field_in_schema(
    kafka_producer: RawProducer, test_settings: AppSettings, caplog
) -> None:
    test_settings.dedup.field = "does_not_exist"

    kafka_producer.produce(test_settings.input.topic.name, value=orjson.dumps(_event("dup-4", 1)))
    kafka_producer.produce(test_settings.input.topic.name, value=orjson.dumps(_event("dup-4", 2)))
    kafka_producer.flush()

    consumer = _make_consumer(test_settings)
    producer = _make_producer(test_settings)
    store = _make_store(test_settings)
    try:
        run_one_iteration(consumer, producer, store, test_settings)
    finally:
        consumer.close()
        store.close()

    # Both rows pass through unfiltered — there's no column to dedup by.
    rows = _drain_output(test_settings, expected=2)
    assert len(rows) == 2


def test_crash_before_flush_does_not_mark_seen_or_commit(tmp_path) -> None:
    """If produce/flush fails, the key must not be marked seen and the offset
    must not be committed — re-processing the same message afterward must not
    treat it as a duplicate."""
    batch = pa.table({"uid": ["crash-uid"], "val": [1]})

    consumer = MagicMock()
    consumer.read_arrow.return_value = batch

    producer = MagicMock()
    producer.produce_arrow = MagicMock()
    producer.flush = MagicMock(side_effect=RuntimeError("boom"))

    store = BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)
    settings = MagicMock()
    settings.input.consumer.batch_size = 100
    settings.input.consumer.batch_timeout_sec = 5
    settings.dedup.field = "uid"

    with pytest.raises(RuntimeError, match="boom"):
        run_one_iteration(consumer, producer, store, settings)

    consumer.commit.assert_not_called()
    assert store.contains(b"crash-uid") is False

    # Simulate a restart: same batch re-read, this time produce succeeds.
    producer2 = MagicMock()
    producer2.produce_arrow = MagicMock()
    producer2.flush = MagicMock()

    run_one_iteration(consumer, producer2, store, settings)

    produced_table = producer2.produce_arrow.call_args[0][0]
    assert len(produced_table) == 1  # not dropped as a duplicate
    consumer.commit.assert_called_once()
    assert store.contains(b"crash-uid") is True

    store.close()
