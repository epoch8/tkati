import uuid
from collections.abc import Generator
from unittest.mock import MagicMock

import clickhouse_connect as ch
import clickhouse_connect.driver as ch_driver
import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tkati_core.clickhouse.settings import (
    ClickHouseConnectionSettings,
    ClickHouseOutputSettings,
    ClickHouseTableSettings,
)
from tkati_core.kafka.settings import (
    KafkaConnectionSettings,
    KafkaConsumerSettings,
    KafkaInputSettings,
    KafkaOutputSettings,
    KafkaTopicSettings,
)
from tkati_core.kafka.testing import kafka_admin_client  # noqa: F401
from tkati_node_el.settings import AppSettings


@pytest.fixture(scope="function")
def run_id() -> str:
    return str(uuid.uuid4())[:8]


@pytest.fixture(scope="function")
def test_settings(run_id: str) -> AppSettings:
    return AppSettings(
        input=KafkaInputSettings(
            connection=KafkaConnectionSettings(broker="localhost:9092"),
            topic=KafkaTopicSettings(
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
        output=ClickHouseOutputSettings(
            connection=ClickHouseConnectionSettings(
                host="localhost",
                port=8123,
                user="default",
                password="default",
                secure=False,
            ),
            table=ClickHouseTableSettings(
                database="default",
                name=f"traffic_event_{run_id}",
            ),
            dlq_split_factor=2,
        ),
        dlq=KafkaOutputSettings(
            connection=KafkaConnectionSettings(broker="localhost:9092"),
            topic=KafkaTopicSettings(name="node-el-dlq"),
        ),
    )


@pytest.fixture(scope="function")
def ch_client(test_settings: AppSettings) -> Generator[ch_driver.Client]:
    """A real clickhouse_connect client, for table setup/teardown and result verification."""
    assert isinstance(test_settings.output, ClickHouseOutputSettings)
    connection = test_settings.output.connection
    client = ch.get_client(
        host=connection.host,
        port=connection.port,
        username=connection.user,
        password=connection.password,
        database=test_settings.output.table.database,
        secure=connection.secure,
    )
    yield client
    client.close()


@pytest.fixture(scope="function")
def ch_table(
    test_settings: AppSettings, ch_client: ch_driver.Client
) -> Generator[str]:
    """Creates the output table against the real ClickHouse instance and drops it afterward."""
    assert isinstance(test_settings.output, ClickHouseOutputSettings)
    table_settings = test_settings.output.table
    table = f"{table_settings.database}.{table_settings.name}"
    ch_client.command(f"""
        CREATE TABLE {table} (
            uid String,
            time DateTime64(3),
            package_id Int32,
            user_hash String,
            sdk_hash String,
            conn_type String,
            country String,
            local_ip String,
            frontend_ip String,
            dest_addr String,
            client_ip String,
            traffic_in UInt32,
            traffic_out UInt32
        ) ENGINE = MergeTree ORDER BY uid
    """)
    yield table_settings.name
    ch_client.command(f"DROP TABLE {table}")


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
) -> Generator[Producer]:
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

    yield Producer({"bootstrap.servers": test_settings.input.connection.broker})

    for f in kafka_admin_client.delete_topics([topic]).values():
        try:
            f.result()
        except Exception as e:
            print(f"Failed to delete topic {topic}: {e}")


@pytest.fixture(scope="function")
def kafka_test_settings(run_id: str) -> AppSettings:
    """A Kafka-in/Kafka-out node-el pipeline (not ClickHouse), for full JSON round-trip tests."""
    schema = {
        "s": "string",
        "time": "timestamp[ms]",
        "i32": "int32",
        "i64": "int64",
        "u32": "uint32",
        "u64": "uint64",
        "u8": "uint8",
    }
    return AppSettings(
        input=KafkaInputSettings(
            connection=KafkaConnectionSettings(broker="localhost:9092"),
            topic=KafkaTopicSettings(
                name=f"e2e_node_el_kafka_in_{run_id}",
                schema=schema,
            ),
            consumer=KafkaConsumerSettings(
                group_id=f"test-node-el-kafka-{run_id}",
                auto_offset_reset="earliest",
                batch_size=100,
                batch_timeout_sec=5,
            ),
        ),
        output=KafkaOutputSettings(
            connection=KafkaConnectionSettings(broker="localhost:9092"),
            topic=KafkaTopicSettings(
                name=f"e2e_node_el_kafka_out_{run_id}",
                schema=schema,
            ),
        ),
    )


@pytest.fixture(scope="function")
def kafka_topics(
    kafka_test_settings: AppSettings,
    kafka_admin_client: AdminClient,  # noqa: F811
) -> Generator[None]:
    """Creates the input and output topics for kafka_test_settings and deletes them afterward."""
    assert isinstance(kafka_test_settings.output, KafkaOutputSettings)
    topics = [kafka_test_settings.input.topic.name, kafka_test_settings.output.topic.name]
    fs = kafka_admin_client.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1) for name in topics]
    )
    for t, f in fs.items():
        try:
            f.result()
        except Exception as e:
            print(f"Failed to create topic {t}: {e}")

    yield

    for f in kafka_admin_client.delete_topics(topics).values():
        try:
            f.result()
        except Exception as e:
            print(f"Failed to delete topic: {e}")


@pytest.fixture(scope="function")
def kafka_producer(
    kafka_test_settings: AppSettings, kafka_topics: None
) -> Generator[Producer]:
    """Yields a raw Producer for seeding the input topic of kafka_test_settings."""
    yield Producer({"bootstrap.servers": kafka_test_settings.input.connection.broker})
