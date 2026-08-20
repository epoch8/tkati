import uuid
from collections.abc import Generator

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tkati_core.kafka.settings import (
    KafkaConnectionSettings,
    KafkaConsumerSettings,
    KafkaInputSettings,
    KafkaOutputSettings,
    KafkaTopicSettings,
)
from tkati_core.kafka.testing import kafka_admin_client  # noqa: F401
from tkati_node_dedup.settings import AppSettings, DedupSettings


@pytest.fixture(scope="function")
def run_id() -> str:
    return str(uuid.uuid4())[:8]


@pytest.fixture(scope="function")
def test_settings(run_id: str, tmp_path) -> AppSettings:
    return AppSettings(
        input=KafkaInputSettings(
            connection=KafkaConnectionSettings(broker="localhost:9092"),
            topic=KafkaTopicSettings(
                name=f"dedup_in_{run_id}",
                schema={"uid": "string", "time": "timestamp[ms]", "val": "int64"},
            ),
            consumer=KafkaConsumerSettings(
                group_id=f"test-node-dedup-{run_id}",
                auto_offset_reset="earliest",
                batch_size=2,
                batch_timeout_sec=2,
            ),
        ),
        output=KafkaOutputSettings(
            connection=KafkaConnectionSettings(broker="localhost:9092"),
            topic=KafkaTopicSettings(name=f"dedup_out_{run_id}"),
        ),
        dedup=DedupSettings(
            field="uid",
            window_hours=3,
            bucket_hours=1,
            store_dir=str(tmp_path / "dedup_store"),
        ),
    )


@pytest.fixture(scope="function")
def kafka_topics(
    test_settings: AppSettings,
    kafka_admin_client: AdminClient,  # noqa: F811
) -> Generator[None]:
    """Creates the input and output topics and deletes them afterward."""
    assert isinstance(test_settings.output, KafkaOutputSettings)
    topics = [test_settings.input.topic.name, test_settings.output.topic.name]
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
    test_settings: AppSettings, kafka_topics: None
) -> Generator[Producer]:
    """Yields a raw Producer for seeding the input topic."""
    yield Producer({"bootstrap.servers": test_settings.input.connection.broker})
