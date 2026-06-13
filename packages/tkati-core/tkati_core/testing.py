from collections.abc import Generator

import pytest
from confluent_kafka.admin import AdminClient


@pytest.fixture(scope="function")
def kafka_admin_client() -> Generator[AdminClient, None, None]:
    """Provides a Kafka AdminClient for integration tests. Requires Redpanda on localhost:9092."""
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    yield admin
