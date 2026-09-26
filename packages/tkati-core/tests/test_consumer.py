import time
from collections.abc import Generator

import orjson
import pyarrow as pa
import pytest
from confluent_kafka import Consumer as RawConsumer
from confluent_kafka import Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from tkati_core import CONSUMER_PHASES, Consumer, LoopStats, build_consumer
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
        assert consumer.wire_schema.field("id").type == pa.string()
        assert consumer.internal_schema.field("value").type == pa.int64()
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
        batch = consumer.read_arrow(
            timeout=5,
            num_messages=10,
        )
    finally:
        consumer.close()

    assert batch is not None
    table = batch.data
    assert len(table) == 3
    assert table.schema.field("id").type == pa.string()
    assert table.schema.field("value").type == pa.int64()
    assert sorted(table.column("id").to_pylist()) == ["a", "b", "c"]
    assert sorted(table.column("value").to_pylist()) == [1, 2, 3]


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
        batch = consumer.read_arrow(
            timeout=5,
            num_messages=3,
        )
    finally:
        consumer.close()

    assert batch is not None
    assert len(batch.data) == 3


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
        batch = consumer.read_arrow(timeout=5, num_messages=1)
    finally:
        consumer.close()

    assert batch is not None
    table = batch.data
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
        batch = consumer.read_pylist(
            timeout=5,
            num_messages=10,
        )
    finally:
        consumer.close()

    assert batch is not None
    rows = batch.data
    assert len(rows) == 3
    assert sorted(r["id"] for r in rows) == ["a", "b", "c"]
    assert sorted(r["value"] for r in rows) == [1, 2, 3]


def test_read_arrow_splits_its_time_into_poll_and_parse(
    input_settings: KafkaInputSettings,
    kafka_input_topic: str,
    raw_producer: Producer,
):
    """Both halves of a read are attributed separately, so a node's perf report
    can say whether it is waiting on the broker or decoding JSON."""
    for i in range(50):
        raw_producer.produce(
            kafka_input_topic, value=orjson.dumps({"id": f"k{i}", "value": i})
        )
    raw_producer.flush()

    stats = LoopStats(phases=CONSUMER_PHASES)
    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        batch = consumer.read_arrow(timeout=5, num_messages=50, stats=stats)
    finally:
        consumer.close()

    assert batch is not None and len(batch.data) == 50
    assert stats.phase_sec["consumer/poll"] > 0
    assert stats.phase_sec["consumer/parse"] > 0


def test_read_arrow_records_only_poll_when_nothing_arrives(
    input_settings: KafkaInputSettings, kafka_input_topic: str
):
    """An empty read returns before the parse block. Charging that wait to
    `parse` would make a starved node look like it was CPU-bound on JSON."""
    stats = LoopStats(phases=CONSUMER_PHASES)
    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        assert consumer.read_arrow(timeout=2, num_messages=10, stats=stats) is None
    finally:
        consumer.close()

    assert stats.phase_sec["consumer/poll"] > 0
    assert "consumer/parse" not in stats.phase_sec


def test_read_pylist_splits_its_time_into_poll_and_parse(
    input_settings: KafkaInputSettings,
    kafka_input_topic: str,
    raw_producer: Producer,
):
    """read_pylist is attributed the same way as read_arrow."""
    for i in range(50):
        raw_producer.produce(
            kafka_input_topic, value=orjson.dumps({"id": f"k{i}", "value": i})
        )
    raw_producer.flush()

    stats = LoopStats(phases=CONSUMER_PHASES)
    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        batch = consumer.read_pylist(timeout=5, num_messages=50, stats=stats)
    finally:
        consumer.close()

    assert batch is not None and len(batch.data) == 50
    assert stats.phase_sec["consumer/poll"] > 0
    assert stats.phase_sec["consumer/parse"] > 0


def test_read_pylist_records_only_poll_when_nothing_arrives(
    input_settings: KafkaInputSettings, kafka_input_topic: str
):
    stats = LoopStats(phases=CONSUMER_PHASES)
    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        assert consumer.read_pylist(timeout=2, num_messages=10, stats=stats) is None
    finally:
        consumer.close()

    assert stats.phase_sec["consumer/poll"] > 0
    assert "consumer/parse" not in stats.phase_sec


def test_read_pylist_is_callable_through_the_base_consumer(
    input_settings: KafkaInputSettings,
    kafka_input_topic: str,
    raw_producer: Producer,
):
    """Callers that only hold the abstract Consumer from build_consumer can
    read dicts without casting to KafkaConsumer."""
    raw_producer.produce(kafka_input_topic, value=orjson.dumps({"id": "a", "value": 1}))
    raw_producer.flush()

    consumer: Consumer = build_consumer(input_settings)
    try:
        batch = consumer.read_pylist(timeout=5, num_messages=1)
    finally:
        consumer.close()

    assert batch is not None
    assert batch.data == [{"id": "a", "value": 1}]


def _produce(producer: Producer, topic: str, ids: list[str], partition: int = 0) -> None:
    for i in ids:
        producer.produce(
            topic, value=orjson.dumps({"id": i, "value": 0}), partition=partition
        )
    producer.flush()


def _ids(consumer: KafkaConsumer, n: int):
    """Read a batch of exactly `n` messages, returning it and its ids."""
    batch = consumer.read_arrow(timeout=10, num_messages=n)
    assert batch is not None and len(batch.data) == n
    return batch, batch.data.column("id").to_pylist()


def _committed(
    settings: KafkaInputSettings, partitions: int = 1
) -> dict[int, int | None]:
    """The group's committed offset per partition, None where there is none."""
    c = RawConsumer(
        {
            "bootstrap.servers": settings.connection.broker,
            "group.id": settings.consumer.group_id,
        }
    )
    try:
        tps = c.committed(
            [TopicPartition(settings.topic.name, p) for p in range(partitions)],
            timeout=10,
        )
    finally:
        c.close()
    return {tp.partition: tp.offset if tp.offset >= 0 else None for tp in tps}


def _wait_committed(
    settings: KafkaInputSettings, expected: dict[int, int | None]
) -> dict[int, int | None]:
    """Commits are async: give one a moment to land before comparing."""
    deadline = time.monotonic() + 10
    while (got := _committed(settings, len(expected))) != expected:
        if time.monotonic() > deadline:
            break
        time.sleep(0.2)
    return got


def test_commit_commits_only_the_batch_given(
    input_settings: KafkaInputSettings,
    kafka_input_topic: str,
    raw_producer: Producer,
):
    """With a second batch already read, committing the first commits the
    first's end, not everything polled so far."""
    _produce(raw_producer, kafka_input_topic, [str(i) for i in range(6)])

    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        b1, _ = _ids(consumer, 3)
        b2, _ = _ids(consumer, 3)

        consumer.commit(b1)
        assert _wait_committed(input_settings, {0: 3}) == {0: 3}

        consumer.commit(b2)
        assert _wait_committed(input_settings, {0: 6}) == {0: 6}
    finally:
        consumer.close()


@pytest.fixture
def three_partition_topic(
    input_settings: KafkaInputSettings, kafka_admin_client: AdminClient
) -> Generator[str]:
    topic = input_settings.topic.name
    for f in kafka_admin_client.create_topics(
        [NewTopic(topic, num_partitions=3, replication_factor=1)]
    ).values():
        f.result()
    yield topic
    for f in kafka_admin_client.delete_topics([topic]).values():
        f.result()


def test_commit_covers_every_partition_in_the_batch(
    input_settings: KafkaInputSettings,
    three_partition_topic: str,
    raw_producer: Producer,
):
    for p in range(3):
        _produce(raw_producer, three_partition_topic, [f"{p}-a", f"{p}-b"], partition=p)

    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        batch, _ = _ids(consumer, 6)
        consumer.commit(batch)
        expected: dict[int, int | None] = {0: 2, 1: 2, 2: 2}
        assert _wait_committed(input_settings, expected) == expected
    finally:
        consumer.close()


def test_rewound_batch_is_read_again(
    input_settings: KafkaInputSettings,
    kafka_input_topic: str,
    raw_producer: Producer,
):
    _produce(raw_producer, kafka_input_topic, ["a", "b", "c"])

    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        b1, first = _ids(consumer, 3)
        consumer.rewind(b1)
        assert _committed(input_settings) == {0: None}

        b2, again = _ids(consumer, 3)
        assert again == first
        consumer.commit(b2)
        assert _wait_committed(input_settings, {0: 3}) == {0: 3}
    finally:
        consumer.close()


def test_rewind_invalidates_the_batches_read_after_it(
    input_settings: KafkaInputSettings,
    kafka_input_topic: str,
    raw_producer: Producer,
):
    """Rewinding b1 re-reads b2's messages too, so b2 itself can no longer be
    committed — that would skip past b1's re-read."""
    _produce(raw_producer, kafka_input_topic, [str(i) for i in range(6)])

    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        b1, first = _ids(consumer, 3)
        b2, _ = _ids(consumer, 3)
        consumer.rewind(b1)

        with pytest.raises(ValueError, match="already committed or rewound"):
            consumer.commit(b2)
        with pytest.raises(ValueError, match="already committed or rewound"):
            consumer.rewind(b2)

        _, again = _ids(consumer, 3)
        assert again == first
    finally:
        consumer.close()


def test_commit_out_of_read_order_raises(
    input_settings: KafkaInputSettings,
    kafka_input_topic: str,
    raw_producer: Producer,
):
    _produce(raw_producer, kafka_input_topic, ["a", "b"])

    consumer = KafkaConsumer.from_input_settings(input_settings)
    try:
        b1, _ = _ids(consumer, 1)
        b2, _ = _ids(consumer, 1)

        with pytest.raises(ValueError, match="batch 0 is the oldest"):
            consumer.commit(b2)
        consumer.commit(b1)
        with pytest.raises(ValueError, match="already committed or rewound"):
            consumer.commit(b1)
        consumer.commit(b2)
        assert _wait_committed(input_settings, {0: 2}) == {0: 2}
    finally:
        consumer.close()
