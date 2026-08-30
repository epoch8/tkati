"""Populate this example's two Kafka topics with sample events, so opening the dashboard and
clicking a node has something real to show in "Latest events". Requires a broker on localhost:9092
(matching this example's `connection.broker`).

Usage: uv run python examples/simple-pipeline/seed_kafka.py
"""

import time

import orjson
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER = "localhost:9092"
RAW_TOPIC = "raw_events"
DEDUPED_TOPIC = "deduped_events"
PAGES = ["/home", "/pricing", "/docs", "/blog/tkati-dashboard", "/signup"]
USERS = ["u1", "u2", "u3", "u4"]


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BROKER})
    for name, future in admin.create_topics(
        [
            NewTopic(RAW_TOPIC, num_partitions=1, replication_factor=1),
            NewTopic(DEDUPED_TOPIC, num_partitions=1, replication_factor=1),
        ]
    ).items():
        try:
            future.result()
            print(f"created topic {name!r}")
        except Exception as e:
            print(f"topic {name!r}: {e} (already exists, continuing)")

    producer = Producer({"bootstrap.servers": BROKER})
    now_ms = int(time.time() * 1000)

    def event(i: int, time_offset_ms: int = 0) -> dict:
        return {
            "event_id": f"evt-{i}",
            "user_id": USERS[i % len(USERS)],
            "time": now_ms + i * 1000 + time_offset_ms,
            "page": PAGES[i % len(PAGES)],
        }

    # Raw events: 10 unique events, with two re-sent as (slightly later) duplicates, so the
    # dedup node's job is visible when you compare raw-events with deduped-events in the panel.
    raw_events = [event(i) for i in range(10)]
    raw_events.append(event(3, time_offset_ms=200))
    raw_events.append(event(7, time_offset_ms=300))
    raw_events.sort(key=lambda e: e["time"])
    for e in raw_events:
        producer.produce(RAW_TOPIC, orjson.dumps(e))

    # Deduped events: the same 10 unique event_ids, as if the dedup node had already run.
    for i in range(10):
        producer.produce(DEDUPED_TOPIC, orjson.dumps(event(i)))

    producer.flush()
    print(
        f"produced {len(raw_events)} events to {RAW_TOPIC!r}, 10 events to {DEDUPED_TOPIC!r}"
    )


if __name__ == "__main__":
    main()
