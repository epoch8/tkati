import time

import orjson
import pyarrow as pa
import pytest
from confluent_kafka import Consumer as RawConsumer
from confluent_kafka import Producer as RawProducer
from prometheus_client import REGISTRY
from tkati_core import SyncNode
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_core.kafka.producer import KafkaProducer
from tkati_core.kafka.settings import KafkaOutputSettings
from tkati_core.testing import memory_node
from tkati_node_dedup.main import _PHASES, run
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


def _drain_output(
    test_settings: AppSettings, expected: int, timeout: float = 10.0
) -> list[dict]:
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


def _run(test_settings: AppSettings, store: BucketedDedupStore) -> None:
    """Run the node over everything already in the input topic, then stop.
    Each call is a fresh consumer in the same group, so a second call resumes
    from the first one's commit — as a restart would."""
    node = SyncNode(
        _make_consumer(test_settings),
        _make_producer(test_settings),
        batch_size=test_settings.input.consumer.batch_size,
        batch_timeout_sec=test_settings.input.consumer.batch_timeout_sec,
        phases=_PHASES,
        stop_when_idle=True,
    )
    with node:
        run(node, store, test_settings.dedup.field)


def _event(uid: str | None, val: int) -> dict:
    return {"uid": uid, "time": int(time.time() * 1000), "val": val}


def test_basic_in_batch_dedup(
    kafka_producer: RawProducer, test_settings: AppSettings
) -> None:
    """Two messages with the same uid produced before one poll: only one survives."""
    kafka_producer.produce(
        test_settings.input.topic.name, value=orjson.dumps(_event("dup-1", 1))
    )
    kafka_producer.produce(
        test_settings.input.topic.name, value=orjson.dumps(_event("dup-1", 2))
    )
    kafka_producer.flush()

    store = _make_store(test_settings)
    try:
        _run(test_settings, store)
    finally:
        store.close()

    rows = _drain_output(test_settings, expected=1)
    assert len(rows) == 1
    assert rows[0]["uid"] == "dup-1"


def test_cross_batch_dedup(
    kafka_producer: RawProducer, test_settings: AppSettings
) -> None:
    """Same uid produced across two separate runs: only the first survives."""
    store = _make_store(test_settings)
    try:
        kafka_producer.produce(
            test_settings.input.topic.name, value=orjson.dumps(_event("dup-2", 1))
        )
        kafka_producer.flush()
        _run(test_settings, store)

        kafka_producer.produce(
            test_settings.input.topic.name, value=orjson.dumps(_event("dup-2", 2))
        )
        kafka_producer.flush()
        _run(test_settings, store)
    finally:
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

    store = _make_store(test_settings)
    try:
        kafka_producer.produce(
            test_settings.input.topic.name, value=orjson.dumps(_event("dup-3", 1))
        )
        kafka_producer.flush()
        _run(test_settings, store)

        # Advance well past window_hours + bucket_hours so the bucket ages out.
        now[0] += 5 * 3600

        kafka_producer.produce(
            test_settings.input.topic.name, value=orjson.dumps(_event("dup-3", 2))
        )
        kafka_producer.flush()
        _run(test_settings, store)
    finally:
        store.close()

    rows = _drain_output(test_settings, expected=2)
    assert len(rows) == 2


def test_null_dedup_field_passes_through(
    kafka_producer: RawProducer, test_settings: AppSettings
) -> None:
    kafka_producer.produce(
        test_settings.input.topic.name, value=orjson.dumps(_event(None, 1))
    )
    kafka_producer.produce(
        test_settings.input.topic.name, value=orjson.dumps(_event(None, 2))
    )
    kafka_producer.flush()

    store = _make_store(test_settings)
    try:
        _run(test_settings, store)
    finally:
        store.close()

    rows = _drain_output(test_settings, expected=2)
    assert len(rows) == 2


def test_missing_dedup_field_in_schema(
    kafka_producer: RawProducer, test_settings: AppSettings, caplog
) -> None:
    test_settings.dedup.field = "does_not_exist"

    kafka_producer.produce(
        test_settings.input.topic.name, value=orjson.dumps(_event("dup-4", 1))
    )
    kafka_producer.produce(
        test_settings.input.topic.name, value=orjson.dumps(_event("dup-4", 2))
    )
    kafka_producer.flush()

    store = _make_store(test_settings)
    try:
        _run(test_settings, store)
    finally:
        store.close()

    # Both rows pass through unfiltered — there's no column to dedup by.
    rows = _drain_output(test_settings, expected=2)
    assert len(rows) == 2


def _store(tmp_path) -> BucketedDedupStore:
    return BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)


def _run_memory(store: BucketedDedupStore, node: SyncNode) -> None:
    with node:
        run(node, store, "uid")


def test_crash_before_flush_does_not_mark_seen_or_commit(tmp_path) -> None:
    """If produce/flush fails, the key must not be marked seen and the batch
    must be rewound rather than committed — re-processing the same message
    afterward must not treat it as a duplicate."""
    table = pa.table({"uid": ["crash-uid"], "val": [1]})
    store = _store(tmp_path)

    node, consumer, _ = memory_node(
        [table], phases=_PHASES, fail_flush=RuntimeError("boom")
    )
    with pytest.raises(RuntimeError, match="boom"):
        _run_memory(store, node)

    assert consumer.commits == []
    assert consumer.rewinds == [0]
    assert store.contains(b"crash-uid") is False

    # Simulate a restart: same batch re-read, this time produce succeeds.
    node, consumer, producer = memory_node([table], phases=_PHASES)
    _run_memory(store, node)

    assert producer is not None
    assert [len(t) for t in producer.sent] == [1]  # not dropped as a duplicate
    assert consumer.commits == [0]
    assert store.contains(b"crash-uid") is True

    store.close()


def test_keys_are_marked_seen_after_delivery_and_commit(tmp_path, monkeypatch) -> None:
    """Deliver, commit, then mark seen: never mark a key seen before its row
    is delivered."""
    store = _store(tmp_path)
    node, consumer, _ = memory_node(
        [pa.table({"uid": ["a"], "val": [1]})], phases=_PHASES
    )
    add_many = store.add_many

    def logged_add_many(keys) -> None:
        consumer.log.append("mark-seen")
        add_many(keys)

    monkeypatch.setattr(store, "add_many", logged_add_many)
    _run_memory(store, node)

    assert consumer.log[:6] == [
        "read:0",
        "produce",
        "flush",
        "wait:1",
        "commit:0",
        "mark-seen",
    ]
    assert store.contains(b"a") is True
    store.close()


def test_idle_event_runs_store_cleanup(tmp_path, monkeypatch) -> None:
    """An idle node must still expire buckets: cleanup runs on empty polls
    too, not only on batches."""
    store = _store(tmp_path)
    calls: list[str] = []
    monkeypatch.setattr(store, "cleanup_expired", lambda: calls.append("cleanup"))

    node, _, _ = memory_node([None, None], phases=_PHASES)
    _run_memory(store, node)

    assert calls == ["cleanup", "cleanup"]
    store.close()


def _dropped_rows_total() -> float:
    value = REGISTRY.get_sample_value("tkati_node_dedup_dropped_rows_total")
    assert value is not None
    return value


def test_dropped_rows_are_counted(tmp_path) -> None:
    """The counter is process-global, so assert on the delta."""
    table = pa.table({"uid": ["a", "a", "b"], "val": [1, 2, 3]})
    store = _store(tmp_path)

    before = _dropped_rows_total()
    node, consumer, producer = memory_node([table], phases=_PHASES)
    _run_memory(store, node)
    assert _dropped_rows_total() - before == 1
    assert producer is not None
    sent = producer.sent[0]
    assert isinstance(sent, pa.Table)
    assert sent.column("uid").to_pylist() == ["a", "b"]
    assert consumer.commits == [0]

    # Same batch again: every row is now a cross-batch duplicate.
    node, _, producer = memory_node([table], phases=_PHASES)
    _run_memory(store, node)
    assert _dropped_rows_total() - before == 4
    assert producer is not None
    assert producer.sent == []

    store.close()


def test_failed_batch_does_not_count_dropped_rows(tmp_path) -> None:
    """A batch that fails before commit is re-read after restart; counting its
    drops on the failed attempt would count them twice."""
    table = pa.table({"uid": ["a", "a"], "val": [1, 2]})
    store = _store(tmp_path)

    before = _dropped_rows_total()
    node, _, _ = memory_node([table], phases=_PHASES, fail_flush=RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        _run_memory(store, node)
    assert _dropped_rows_total() == before

    store.close()


def test_metrics_are_served_by_default(test_settings: AppSettings) -> None:
    """On unless a deployment opts out, on the port the README documents."""
    assert test_settings.metrics.enabled is True
    assert test_settings.metrics.port == 8000
