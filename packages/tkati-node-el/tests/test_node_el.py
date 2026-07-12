import time
from unittest.mock import MagicMock

import orjson
import pyarrow as pa
import pytest
from confluent_kafka import Producer
from tkati_core.clickhouse.producer import ClickhouseProducer
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_node_el.main import run_one_iteration
from tkati_node_el.settings import AppSettings


def _make_consumer(test_settings: AppSettings) -> KafkaConsumer:
    return KafkaConsumer(
        kafka_config={
            "bootstrap.servers": test_settings.input.topic.broker,
            "group.id": test_settings.input.consumer.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        },
        topic_name=test_settings.input.topic.name,
        input_schema=test_settings.input.topic.schema,
    )


def _make_ch_producer(mock_ch_client: MagicMock, mock_dlq_producer: MagicMock, table: str = "traffic_event") -> ClickhouseProducer:
    return ClickhouseProducer(ch_client=mock_ch_client, table=table, dlq_producer=mock_dlq_producer)


def test_node_el_valid_flow(
    kafka_producer_and_topic: Producer,
    mock_ch_client: MagicMock,
    mock_dlq_producer: MagicMock,
    test_settings: AppSettings,
) -> None:
    """Produce a valid event to Kafka, run one iteration, verify CH insert was called."""
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

    consumer = _make_consumer(test_settings)
    ch_producer = ClickhouseProducer(
        ch_client=mock_ch_client,
        table=test_settings.output.table,
        dlq_producer=mock_dlq_producer,
    )
    try:
        run_one_iteration(consumer, ch_producer, test_settings)
    finally:
        consumer.close()

    mock_ch_client.insert_arrow.assert_called_once()
    _, kwargs = mock_ch_client.insert_arrow.call_args
    assert kwargs["table"] == test_settings.output.table
    result_table = kwargs["arrow_table"]
    assert isinstance(result_table, pa.Table)
    assert len(result_table) == 1
    assert result_table.column("uid")[0].as_py() == "abc123"
    mock_dlq_producer.produce_arrow.assert_not_called()


def test_node_el_malformed_data(
    kafka_producer_and_topic: Producer,
    mock_ch_client: MagicMock,
    mock_dlq_producer: MagicMock,
    test_settings: AppSettings,
) -> None:
    """Produce malformed JSON to Kafka; run_one_iteration must raise with 'JSON parse error'."""
    kafka_producer_and_topic.produce(
        test_settings.input.topic.name, value=b"not a json object"
    )
    kafka_producer_and_topic.flush()

    consumer = _make_consumer(test_settings)
    ch_producer = ClickhouseProducer(
        ch_client=mock_ch_client,
        table=test_settings.output.table,
        dlq_producer=mock_dlq_producer,
    )
    try:
        with pytest.raises(Exception, match="JSON parse error"):
            run_one_iteration(consumer, ch_producer, test_settings)
    finally:
        consumer.close()

    mock_ch_client.insert_arrow.assert_not_called()
