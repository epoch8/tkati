"""Live partitioning/replication/retention lookup for a kafka-topic node, for the dashboard's
node panel.

Like snapshot.py and lag.py, this is a best-effort, on-demand look at a live broker — it never
subscribes, polls, or joins any consumer group, only reads cluster/topic metadata and topic
config, both read-only broker operations.
"""

import uuid
from typing import Any

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, ConfigResource

from tkati_dashboard._kafka_metadata import resolve_topic_metadata

DEFAULT_TIMEOUT_SEC = 5.0

# Topic-level configs worth surfacing in the panel; describe_configs returns many more
# low-level/rarely-relevant entries we don't show.
INTERESTING_CONFIGS = (
    "retention.ms",
    "retention.bytes",
    "cleanup.policy",
    "min.insync.replicas",
    "segment.bytes",
    "compression.type",
    "max.message.bytes",
)


class TopicStatsError(RuntimeError):
    """Topic stats could not be fetched."""


def fetch_topic_stats(
    broker: str,
    topic: str,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
) -> dict[str, Any]:
    """Return `topic`'s partition layout (leader/replicas/in-sync-replicas per partition) and
    its topic-level config (retention, cleanup policy, etc.)."""
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": f"tkati-dashboard-topic-stats-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        try:
            topic_metadata = resolve_topic_metadata(
                consumer, broker, topic, timeout_sec
            )
        except RuntimeError as e:
            raise TopicStatsError(str(e)) from e

        raw_partitions = sorted(topic_metadata.partitions.items())
        partitions = [
            {
                "partition": pid,
                "leader": p.leader,
                "replicas": list(p.replicas),
                "isrs": list(p.isrs),
                "under_replicated": len(p.isrs) < len(p.replicas),
            }
            for pid, p in raw_partitions
        ]
        replication_factor = len(raw_partitions[0][1].replicas) if raw_partitions else 0
    finally:
        consumer.close()

    return {
        "partition_count": len(partitions),
        "replication_factor": replication_factor,
        "partitions": partitions,
        "config": _fetch_topic_config(broker, topic, timeout_sec),
    }


def _fetch_topic_config(broker: str, topic: str, timeout_sec: float) -> dict[str, Any]:
    admin = AdminClient({"bootstrap.servers": broker})
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    (future,) = admin.describe_configs([resource], request_timeout=timeout_sec).values()

    try:
        entries = future.result(timeout=timeout_sec)
    except Exception as e:
        raise TopicStatsError(f"Could not fetch config for topic {topic!r}: {e}") from e

    return {
        name: {"value": entries[name].value, "is_default": entries[name].is_default}
        for name in INTERESTING_CONFIGS
        if name in entries
    }
