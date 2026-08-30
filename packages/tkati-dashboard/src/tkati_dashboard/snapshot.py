"""Live peek at the most recent messages on a Kafka topic, for the dashboard's node panel.

This is the one place tkati-dashboard talks to a live broker rather than just the serialized
dataflow directory — it's an on-demand, best-effort convenience for the UI, not something the
graph view depends on.
"""

import time
import uuid
from typing import Any

import orjson
from confluent_kafka import Consumer, TopicPartition

from tkati_dashboard._kafka_metadata import resolve_partitions

DEFAULT_LIMIT = 20
DEFAULT_TIMEOUT_SEC = 5.0

# confluent_kafka.Message.timestamp()'s first element, per librdkafka's
# rd_kafka_timestamp_type_t.
TIMESTAMP_TYPES = {0: "unavailable", 1: "create", 2: "log_append"}


class SnapshotError(RuntimeError):
    """A live topic snapshot could not be fetched."""


def fetch_kafka_snapshot(
    broker: str,
    topic: str,
    limit: int = DEFAULT_LIMIT,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
) -> list[dict[str, Any]]:
    """Read up to `limit` of the most recent messages from `topic`, oldest first.

    Each returned dict is `{partition, offset, timestamp, timestamp_type, value}` — Kafka-level
    metadata alongside the message's own JSON-parsed body (`value`), since the two aren't
    otherwise distinguishable once decoded. `timestamp` is epoch milliseconds, or `None` if the
    broker didn't record one (`timestamp_type` is then `"unavailable"`).

    Uses a throwaway consumer group and explicit offset seeking to each partition's tail,
    rather than joining any real pipeline's consumer group — this never affects committed
    offsets or steals messages from an active consumer.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": f"tkati-dashboard-snapshot-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        try:
            partition_ids = resolve_partitions(consumer, broker, topic, timeout_sec)
        except RuntimeError as e:
            raise SnapshotError(str(e)) from e

        tails: list[tuple[TopicPartition, int]] = []
        for partition_id in partition_ids:
            tp = TopicPartition(topic, partition_id)
            low, high = consumer.get_watermark_offsets(
                tp, timeout=timeout_sec, cached=False
            )
            tp.offset = max(low, high - limit)
            tails.append((tp, high))

        want = sum(high - tp.offset for tp, high in tails)
        if want == 0:
            return []

        consumer.assign([tp for tp, _ in tails])

        messages = []
        deadline = time.monotonic() + timeout_sec
        while len(messages) < want and time.monotonic() < deadline:
            msg = consumer.poll(timeout=min(1.0, deadline - time.monotonic()))
            if msg is None or msg.error():
                continue
            messages.append(msg)

        # Across partitions, arrival order isn't wall-clock order; sort by timestamp when the
        # broker recorded one so "latest N" reads chronologically in the UI.
        messages.sort(key=lambda m: m.timestamp()[1])

        events = []
        for msg in messages[-limit:]:
            value = msg.value()
            if value is None:
                parsed: Any = {"_raw": None}  # tombstone (null-value) message
            else:
                try:
                    parsed = orjson.loads(value)
                except Exception:
                    parsed = {"_raw": value.decode("utf-8", "replace")}

            timestamp_type, timestamp_ms = msg.timestamp()
            events.append(
                {
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": timestamp_ms if timestamp_ms >= 0 else None,
                    "timestamp_type": TIMESTAMP_TYPES.get(
                        timestamp_type, "unavailable"
                    ),
                    "value": parsed,
                }
            )
        return events
    finally:
        consumer.close()
