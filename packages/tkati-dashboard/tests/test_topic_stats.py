"""Real Kafka round trip for fetch_topic_stats. Requires a broker on localhost:9092
(see tests/conftest.py's kafka_topic fixture), same as the e2e tests in tkati-node-el/-dedup.
"""

import pytest
from tkati_dashboard.topic_stats import TopicStatsError, fetch_topic_stats

BROKER = "localhost:9092"


def test_fetch_topic_stats_reports_partitions_and_config(kafka_topic: str) -> None:
    result = fetch_topic_stats(BROKER, kafka_topic)

    assert result["partition_count"] == 1
    assert result["replication_factor"] == 1
    assert result["partitions"] == [
        {
            "partition": 0,
            "leader": 0,
            "replicas": [0],
            "isrs": [0],
            "under_replicated": False,
        }
    ]
    assert result["config"]["cleanup.policy"] == {"value": "delete", "is_default": True}
    assert "retention.ms" in result["config"]


def test_fetch_topic_stats_unreachable_broker_raises() -> None:
    with pytest.raises(TopicStatsError):
        fetch_topic_stats("127.0.0.1:1", "some-topic", timeout_sec=1.0)
