import uuid
from collections.abc import Generator

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tkati_core.settings import (
    KafkaConsumerSettings,
    KafkaInputSettings,
    KafkaOutputSettings,
    KafkaTopicSettings,
)
from tkati_core.testing import kafka_admin_client  # noqa: F401

BROKER = "localhost:9092"


@pytest.fixture(scope="function")
def run_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="function")
def input_settings(run_id: str) -> KafkaInputSettings:
    return KafkaInputSettings(
        topic=KafkaTopicSettings(
            broker=BROKER,
            name=f"test_consumer_{run_id}",
            schema={"id": "string", "value": "int64"},
        ),
        consumer=KafkaConsumerSettings(
            group_id=f"test-group-{run_id}",
            auto_offset_reset="earliest",
            batch_size=100,
            batch_timeout_sec=3,
        ),
    )


@pytest.fixture(scope="function")
def output_settings(run_id: str) -> KafkaOutputSettings:
    return KafkaOutputSettings(
        topic=KafkaTopicSettings(
            broker=BROKER,
            name=f"test_producer_{run_id}",
        )
    )


@pytest.fixture(scope="function")
def kafka_input_topic(
    input_settings: KafkaInputSettings,
    kafka_admin_client: AdminClient,  # noqa: F811
) -> Generator[str, None, None]:
    """Creates the input topic and yields its name."""
    topic = input_settings.topic.name
    fs = kafka_admin_client.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    )
    for t, f in fs.items():
        try:
            f.result()
        except Exception as e:
            print(f"Failed to create topic {t}: {e}")
    yield topic


@pytest.fixture(scope="function")
def kafka_output_topic(
    output_settings: KafkaOutputSettings,
    kafka_admin_client: AdminClient,  # noqa: F811
) -> Generator[str, None, None]:
    """Creates the output topic and yields its name."""
    topic = output_settings.topic.name
    fs = kafka_admin_client.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    )
    for t, f in fs.items():
        try:
            f.result()
        except Exception as e:
            print(f"Failed to create topic {t}: {e}")
    yield topic


@pytest.fixture(scope="function")
def raw_producer() -> Generator[Producer, None, None]:
    """A raw confluent Producer for seeding test messages."""
    p = Producer({"bootstrap.servers": BROKER})
    yield p
    p.flush()


@pytest.fixture(scope="function")
def raw_consumer(run_id: str) -> Generator[Consumer, None, None]:
    """A raw confluent Consumer for verifying produced messages."""
    c = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": f"test-verify-{run_id}",
            "auto.offset.reset": "earliest",
        }
    )
    yield c
    c.close()
