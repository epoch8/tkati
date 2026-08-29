"""Live consumer-group lag lookup for a dataflow edge, for the dashboard's edge labels.

Like tkati_dashboard.snapshot, this is a best-effort, on-demand look at a live broker — it never
subscribes or polls as the consumer group, only reads its committed offsets via `.committed()`,
so it can never join the group, trigger a rebalance, or otherwise disrupt a real pipeline.
"""

from typing import Any

from confluent_kafka import Consumer, TopicPartition

from tkati_dashboard._kafka_metadata import resolve_partitions

DEFAULT_TIMEOUT_SEC = 5.0


class LagError(RuntimeError):
    """Consumer lag could not be computed."""


def fetch_consumer_lag(
    broker: str,
    topic: str,
    group_id: str,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
) -> dict[str, Any]:
    """Return per-partition and total lag of `group_id` on `topic`.

    A partition `group_id` has never committed an offset on counts as fully behind (lag =
    partition size), since that's the backlog the group would have to process from scratch.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        try:
            partition_ids = resolve_partitions(consumer, broker, topic, timeout_sec)
        except RuntimeError as e:
            raise LagError(str(e)) from e

        if not partition_ids:
            return {"total_lag": 0, "partitions": []}

        try:
            committed = consumer.committed(
                [TopicPartition(topic, pid) for pid in partition_ids],
                timeout=timeout_sec,
            )
        except Exception as e:
            raise LagError(
                f"Could not fetch committed offsets for group {group_id!r}: {e}"
            ) from e

        partitions = []
        total_lag = 0
        for tp in committed:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, tp.partition), timeout=timeout_sec, cached=False
            )
            has_committed = tp.offset is not None and tp.offset >= 0
            current = tp.offset if has_committed else low
            partition_lag = max(high - current, 0)
            total_lag += partition_lag
            partitions.append(
                {
                    "partition": tp.partition,
                    "committed_offset": tp.offset if has_committed else None,
                    "high_watermark": high,
                    "lag": partition_lag,
                }
            )
        return {"total_lag": total_lag, "partitions": partitions}
    finally:
        consumer.close()
