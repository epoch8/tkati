import time
from unittest.mock import MagicMock

import clickhouse_connect.driver as ch_driver
import orjson
import pytest
from confluent_kafka import Producer
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
