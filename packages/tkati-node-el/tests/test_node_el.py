import time
from unittest.mock import MagicMock

import clickhouse_connect.driver as ch_driver
import orjson
import pyarrow as pa
import pytest
from confluent_kafka import Producer
from tkati_core import LoopStats
from tkati_core.clickhouse.producer import ClickhouseProducer
from tkati_core.clickhouse.settings import ClickHouseOutputSettings
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_node_el.main import run_one_iteration
from tkati_node_el.settings import AppSettings


def _make_consumer(test_settings: AppSettings) -> KafkaConsumer:
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


def test_node_el_valid_flow(
    kafka_producer_and_topic: Producer,
    ch_client: ch_driver.Client,
    ch_table: str,
    mock_dlq_producer: MagicMock,
    test_settings: AppSettings,
) -> None:
    """Produce a valid event to Kafka, run one iteration, verify the row lands in ClickHouse."""
    event = {
        "uid": "abc123",
        "time": int(time.time() * 1000),
        "package_id": 1,
        "user_hash": "uhash",
        "sdk_hash": "shash",
        "conn_type": "https",
        "country": "US",
        "local_ip": "10.0.0.1",
        "frontend_ip": "1.2.3.4",
        "dest_addr": "8.8.8.8",
        "client_ip": "192.168.1.1",
        "traffic_in": 100,
        "traffic_out": 200,
    }

    kafka_producer_and_topic.produce(
        test_settings.input.topic.name, value=orjson.dumps(event)
    )
    kafka_producer_and_topic.flush()

    assert isinstance(test_settings.output, ClickHouseOutputSettings)

    consumer = _make_consumer(test_settings)
    ch_producer = ClickhouseProducer(
        ch_client=ch_client,
        table=ch_table,
        dlq_producer=mock_dlq_producer,
    )
    try:
        run_one_iteration(consumer, ch_producer, test_settings)
    finally:
        consumer.close()

    result = ch_client.query(f"SELECT uid, traffic_in, traffic_out FROM {ch_table}")
    assert result.result_rows == [("abc123", 100, 200)]
    mock_dlq_producer.produce_arrow.assert_not_called()


def test_node_el_malformed_data(
    kafka_producer_and_topic: Producer,
    ch_client: ch_driver.Client,
    ch_table: str,
    mock_dlq_producer: MagicMock,
    test_settings: AppSettings,
) -> None:
    """Produce malformed JSON to Kafka; run_one_iteration must raise with 'JSON parse error'."""
    kafka_producer_and_topic.produce(
        test_settings.input.topic.name, value=b"not a json object"
    )
    kafka_producer_and_topic.flush()

    assert isinstance(test_settings.output, ClickHouseOutputSettings)

    consumer = _make_consumer(test_settings)
    ch_producer = ClickhouseProducer(
        ch_client=ch_client,
        table=ch_table,
        dlq_producer=mock_dlq_producer,
    )
    try:
        with pytest.raises(Exception, match="JSON parse error"):
            run_one_iteration(consumer, ch_producer, test_settings)
    finally:
        consumer.close()

    result = ch_client.query(f"SELECT count() FROM {ch_table}")
    assert result.result_rows[0][0] == 0


def _mock_settings(batch_size: int = 100) -> MagicMock:
    settings = MagicMock()
    settings.input.consumer.batch_size = batch_size
    settings.input.consumer.batch_timeout_sec = 5
    return settings


def test_iteration_hands_its_stats_to_the_consumer_and_producer() -> None:
    """The node times no read or produce phase of its own; it relies on the
    consumer and producer to fill in theirs. If it stopped passing `stats`
    down, those report columns would silently read 0.00s."""
    consumer = MagicMock()
    consumer.read_arrow.return_value = pa.table({"uid": ["a", "b"]})
    producer = MagicMock()

    stats = LoopStats(phases=())
    run_one_iteration(consumer, producer, _mock_settings(), stats)

    assert consumer.read_arrow.call_args.kwargs["stats"] is stats
    assert producer.produce_arrow.call_args.kwargs["stats"] is stats
    assert producer.flush.call_args.kwargs["stats"] is stats


def test_failed_flush_does_not_commit() -> None:
    """A Kafka output only enqueues in produce_arrow; the offset must not be
    committed until flush confirms delivery, or a crash loses the batch."""
    consumer = MagicMock()
    consumer.read_arrow.return_value = pa.table({"uid": ["a"]})
    producer = MagicMock()
    producer.flush.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        run_one_iteration(consumer, producer, _mock_settings())

    consumer.commit.assert_not_called()


def test_iteration_counts_rows_and_starved_iterations() -> None:
    """A full batch is not starved; a short batch or no batch at all is."""
    consumer = MagicMock()
    producer = MagicMock()
    settings = _mock_settings(batch_size=2)
    stats = LoopStats(phases=())

    for batch in (pa.table({"uid": ["a", "b"]}), pa.table({"uid": ["c"]}), None):
        consumer.read_arrow.return_value = batch
        run_one_iteration(consumer, producer, settings, stats)

    assert stats.iterations == 3
    assert stats.starved_iterations == 2
    assert stats.rows_in == 3
    assert stats.rows_out == 3
    assert consumer.commit.call_count == 2


def test_metrics_are_served_by_default(test_settings: AppSettings) -> None:
    """On unless a deployment opts out, on the port the README documents."""
    assert test_settings.metrics.enabled is True
    assert test_settings.metrics.port == 8000
