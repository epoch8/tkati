"""Shared "resolve a live topic's metadata" helpers for snapshot.py, lag.py, and topic_stats.py."""

from typing import Any

from confluent_kafka import Consumer


def resolve_topic_metadata(
    consumer: Consumer, broker: str, topic: str, timeout_sec: float
) -> Any:
    """Return `topic`'s TopicMetadata (partitions, each with leader/replicas/isrs), or raise
    RuntimeError with a message naming `broker`/`topic` if the broker is unreachable or the
    topic doesn't exist.
    """
    try:
        metadata = consumer.list_topics(topic, timeout=timeout_sec)
    except Exception as e:
        raise RuntimeError(f"Could not reach broker {broker!r}: {e}") from e

    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise RuntimeError(f"Topic {topic!r} not found on {broker!r}")

    return topic_metadata


def resolve_partitions(
    consumer: Consumer, broker: str, topic: str, timeout_sec: float
) -> list[int]:
    """Return the partition ids of `topic`. See resolve_topic_metadata for error behavior."""
    return list(resolve_topic_metadata(consumer, broker, topic, timeout_sec).partitions)
