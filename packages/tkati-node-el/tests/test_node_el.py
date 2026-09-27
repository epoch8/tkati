import time
from unittest.mock import MagicMock

import clickhouse_connect.driver as ch_driver
import orjson
import pyarrow as pa
import pytest
from confluent_kafka import Producer
from tkati_core import SyncNode
from tkati_core.clickhouse.producer import ClickhouseProducer
from tkati_core.clickhouse.settings import ClickHouseOutputSettings
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_core.producer import Producer as ProducerBase
from tkati_core.testing import memory_node
from tkati_node_el.main import run
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


def _run(test_settings: AppSettings, producer: ProducerBase) -> None:
    """Run the node over everything already in the input topic, then stop."""
    node = SyncNode(
        _make_consumer(test_settings),
        producer,
        batch_size=test_settings.input.consumer.batch_size,
        batch_timeout_sec=test_settings.input.consumer.batch_timeout_sec,
        stop_when_idle=True,
    )
    with node:
        run(node)


def _ch_producer(test_settings: AppSettings, dlq: MagicMock) -> ClickhouseProducer:
    """A producer with its own client: the node closes it, and the `ch_client`
    fixture is still needed afterwards to check the table."""
    assert isinstance(test_settings.output, ClickHouseOutputSettings)
    return ClickhouseProducer.from_output_settings(
        test_settings.output, dlq_producer=dlq
    )


def test_node_el_valid_flow(
    kafka_producer_and_topic: Producer,
    ch_client: ch_driver.Client,
    ch_table: str,
    mock_dlq_producer: MagicMock,
    test_settings: AppSettings,
) -> None:
    """Produce a valid event to Kafka, run the node, verify the row lands in ClickHouse."""
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

    _run(test_settings, _ch_producer(test_settings, mock_dlq_producer))

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
    """Produce malformed JSON to Kafka; the node must raise with 'JSON parse error'."""
    kafka_producer_and_topic.produce(
        test_settings.input.topic.name, value=b"not a json object"
    )
    kafka_producer_and_topic.flush()

    producer = _ch_producer(test_settings, mock_dlq_producer)
    with pytest.raises(Exception, match="JSON parse error"):
        _run(test_settings, producer)

    result = ch_client.query(f"SELECT count() FROM {ch_table}")
    assert result.result_rows[0][0] == 0


def test_every_batch_is_sent_unchanged() -> None:
    tables = [pa.table({"uid": ["a", "b"]}), pa.table({"uid": ["c"]})]
    node, consumer, producer = memory_node(tables)
    with node:
        run(node)

    assert producer is not None
    assert producer.sent == tables
    assert consumer.commits == [0, 1]


def test_metrics_are_served_by_default(test_settings: AppSettings) -> None:
    """On unless a deployment opts out, on the port the README documents."""
    assert test_settings.metrics.enabled is True
    assert test_settings.metrics.port == 8000
