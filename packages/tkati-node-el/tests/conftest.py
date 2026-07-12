import uuid
from collections.abc import Generator
from unittest.mock import MagicMock

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tkati_core.kafka.settings import (
    KafkaConsumerSettings,
    KafkaTopicSettings,
)
from tkati_core.kafka.testing import kafka_admin_client  # noqa: F401
from tkati_node_el.settings import AppSettings, ClickHouseOutputConfig, DLQSettings, KafkaInputConfig


@pytest.fixture(scope="function")
def test_settings() -> AppSettings:
    run_id = str(uuid.uuid4())[:8]
    return AppSettings(
        input=KafkaInputConfig(
            topic=KafkaTopicSettings(
                broker="localhost:9092",
                name=f"e2e_node_el_{run_id}",
                schema={
                    "uid": "string",
                    "time": "timestamp[ms]",
                    "package_id": "int32",
                    "user_hash": "string",
                    "sdk_hash": "string",
                    "conn_type": "string",
                    "country": "string",
                    "local_ip": "string",
                    "frontend_ip": "string",
                    "dest_addr": "string",
                    "client_ip": "string",
                    "traffic_in": "uint32",
                    "traffic_out": "uint32",
                },
            ),
            consumer=KafkaConsumerSettings(
                group_id=f"test-node-el-{run_id}",
                auto_offset_reset="earliest",
                batch_size=100,
                batch_timeout_sec=5,
            ),
        ),
        output=ClickHouseOutputConfig(
            host="localhost",
            port=8123,
            user="default",
            password="",
            database="default",
            table="traffic_event",
            secure=False,
        ),
        dlq=DLQSettings(
            topic=KafkaTopicSettings(broker="localhost:9092", name="node-el-dlq"),
            split_factor=2,
        ),
    )


@pytest.fixture(scope="function")
def mock_ch_client() -> MagicMock:
    """Mock for clickhouse_connect client. insert_arrow is the only called method."""
    client = MagicMock()
    client.insert_arrow = MagicMock()
    return client


@pytest.fixture(scope="function")
def mock_dlq_producer() -> MagicMock:
    producer = MagicMock()
    producer.produce_arrow = MagicMock()
    producer.flush = MagicMock()
    return producer


@pytest.fixture(scope="function")
def kafka_producer_and_topic(
    test_settings: AppSettings,
    kafka_admin_client: AdminClient,  # noqa: F811
) -> Generator[Producer, None, None]:
    """Creates the input topic and yields a Producer for it."""
    topic = test_settings.input.topic.name
    fs = kafka_admin_client.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    )
    for t, f in fs.items():
        try:
            f.result()
        except Exception as e:
            print(f"Failed to create topic {t}: {e}")

    yield Producer({"bootstrap.servers": test_settings.input.topic.broker})
