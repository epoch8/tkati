"""End-to-end throughput of the node loop modes, Kafka to Kafka.

Runs the tkati-node-el loop (send every batch unchanged) over the same input
topic three ways, each with a fresh consumer group and output topic:

- `SyncNode`, `read_ahead=0`: the 0.7.0 loop, one step after another;
- `SyncNode`, `read_ahead=1`: the next batch is polled and parsed while the
  current one is produced and flushed;
- `PipelinedNode`: read-ahead, and `done()` doesn't wait for delivery.

Needs a broker (Redpanda) on localhost:9092.

    uv run python packages/tkati-core/benchmarks/bench_node_pipeline.py [--messages 200000]
"""

import argparse
import random
import time
import uuid

import orjson
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tkati_core import Batch, PipelinedNode, SyncNode
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_core.kafka.producer import KafkaProducer

BROKER = "localhost:9092"

# tkati-node-el's production-shaped input schema, as in bench_kafka_json.py.
SCHEMA = {
    "uid": "string",
    "time": "timestamp[ms]",
    "package_id": "int32",
    "user_hash": "string",
    "sdk_hash": "string",
    "conn_type": "string",
    "country": "string",
    "local_ip": "string",
    "frontend_ip": "string",
    "dest_addr": "string",
    "client_ip": "string",
    "traffic_in": "uint32",
    "traffic_out": "uint32",
}


def _event(i: int) -> bytes:
    return orjson.dumps(
        {
            "uid": f"uid-{i}",
            "time": 1_700_000_000_000 + i,
            "package_id": i % 1000,
            "user_hash": f"user-{random.getrandbits(32):08x}",
            "sdk_hash": f"sdk-{i % 50}",
            "conn_type": "https",
            "country": random.choice(["US", "DE", "BR", "IN"]),
            "local_ip": "10.0.0.1",
            "frontend_ip": "1.2.3.4",
            "dest_addr": "8.8.8.8",
            "client_ip": "192.168.1.1",
            "traffic_in": random.randrange(1 << 20),
            "traffic_out": random.randrange(1 << 20),
        }
    )


def _seed(admin: AdminClient, topic: str, messages: int) -> None:
    for f in admin.create_topics([NewTopic(topic, 1, 1)]).values():
        f.result()
    producer = Producer({"bootstrap.servers": BROKER, "linger.ms": 50})
    for i in range(messages):
        while True:
            try:
                producer.produce(topic, _event(i))
                break
            except BufferError:
                producer.poll(0.1)
    producer.flush()


def _run(mode: str, input_topic: str, messages: int, batch_size: int) -> float:
    """Rows per second for one mode, from the first poll to the exit."""
    run_id = uuid.uuid4().hex[:8]
    output_topic = f"bench_out_{run_id}"
    consumer = KafkaConsumer(
        kafka_config={
            "bootstrap.servers": BROKER,
            "group.id": f"bench-{run_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        },
        topic_name=input_topic,
        input_schema=SCHEMA,
    )
    producer = KafkaProducer(
        kafka_config={"bootstrap.servers": BROKER}, topic_name=output_topic
    )
    node: SyncNode | PipelinedNode
    if mode == "pipelined":
        node = PipelinedNode(
            consumer,
            producer,
            batch_size=batch_size,
            batch_timeout_sec=5,
            read_ahead=1,
            max_in_flight=4,
        )
    else:
        node = SyncNode(
            consumer,
            producer,
            batch_size=batch_size,
            batch_timeout_sec=5,
            read_ahead=0 if mode == "sync" else 1,
        )

    started = time.perf_counter()
    rows = 0
    with node:
        for event in node.consume_arrow():
            if isinstance(event, Batch):
                node.done(event, output_arrow=event.data)
                rows += len(event.data)
                if rows >= messages:
                    node.stop()
    elapsed = time.perf_counter() - started
    return rows / elapsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--messages", type=int, default=200_000)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": BROKER})
    input_topic = f"bench_in_{uuid.uuid4().hex[:8]}"
    print(f"seeding {args.messages} messages into {input_topic} ...")
    _seed(admin, input_topic, args.messages)

    modes = ("sync", "sync+read-ahead", "pipelined")
    results: dict[str, list[float]] = {mode: [] for mode in modes}
    try:
        for _ in range(args.repeat):
            for mode in modes:
                results[mode].append(
                    _run(mode, input_topic, args.messages, args.batch_size)
                )
    finally:
        admin.delete_topics([input_topic])

    print(f"{args.messages} messages, batch_size={args.batch_size}, best of {args.repeat}:")
    base = max(results["sync"])
    for mode in modes:
        best = max(results[mode])
        print(f"  {mode:<16} {best:>10,.0f} rows/s  ({best / base:.2f}x)")


if __name__ == "__main__":
    main()
