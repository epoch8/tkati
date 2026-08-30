import uuid
from collections.abc import Generator
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from tkati_core.kafka.testing import kafka_admin_client  # noqa: F401

DATA_DIR = Path(__file__).parent / "data"
KAFKA_BROKER = "localhost:9092"


@pytest.fixture
def sample_dataflow_dir() -> Path:
    return DATA_DIR / "sample-dataflow"


@pytest.fixture(
    params=sorted(p.name for p in DATA_DIR.iterdir() if p.name.startswith("invalid-"))
)
def invalid_dataflow_dir(request: pytest.FixtureRequest) -> Path:
    return DATA_DIR / request.param


@pytest.fixture
def kafka_topic(kafka_admin_client: AdminClient) -> Generator[str]:  # noqa: F811
    """A throwaway real Kafka topic (single partition), for the live snapshot/lag tests.
    Requires a broker on KAFKA_BROKER."""
    name = f"tkati_dashboard_test_{uuid.uuid4().hex[:8]}"
    for f in kafka_admin_client.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1)]
    ).values():
        f.result()

    yield name

    for f in kafka_admin_client.delete_topics([name]).values():
        f.result()
