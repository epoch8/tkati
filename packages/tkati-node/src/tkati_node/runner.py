"""Kafka consumer/producer loop with batching and at-least-once offset management."""

from __future__ import annotations

import importlib
import logging
import time
from collections import defaultdict
from typing import Any

import pyarrow as pa
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from tkati_node.base import ManyToMany, OneToMany, OneToOne, ZeroToOne
from tkati_node.env import InputConfig, NodeConfig, OutputConfig
from tkati_node.serde import (
    deserialize_arrow,
    deserialize_json_messages,
    serialize_arrow,
    serialize_json_messages,
)

log = logging.getLogger("tkati_node")

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

_RECORDS_CONSUMED = Counter(
    "tkati_records_consumed_total",
    "Records consumed from input topics",
    ["topic"],
)
_RECORDS_PRODUCED = Counter(
    "tkati_records_produced_total",
    "Records produced to output topics",
    ["topic"],
)
_HANDLER_DURATION = Histogram(
    "tkati_handler_duration_seconds",
    "Node method call latency",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)
_CONSUMER_LAG = Gauge(
    "tkati_consumer_lag_records",
    "Current consumer lag per input topic",
    ["topic"],
)
_HANDLER_ERRORS = Counter(
    "tkati_handler_errors_total",
    "Number of unhandled exceptions",
)


# ---------------------------------------------------------------------------
# Handler loading
# ---------------------------------------------------------------------------

def _load_handler(dotted_path: str, config: dict) -> Any:
    """Import the module and instantiate the handler class."""
    module_path, class_name = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(config)


# ---------------------------------------------------------------------------
# Producing helpers
# ---------------------------------------------------------------------------

def _produce(
    producer: KafkaProducer,
    outputs_by_topic: dict[str, OutputConfig],
    output_schemas: dict[str, pa.Schema],
    result: pa.RecordBatch | dict[str, pa.RecordBatch] | None,
    output_configs: list[OutputConfig],
) -> None:
    if result is None:
        return

    if isinstance(result, pa.RecordBatch):
        # OneToOne: single output
        if len(output_configs) != 1:
            raise RuntimeError("OneToOne handler must have exactly one output configured")
        _produce_one(producer, output_configs[0], output_schemas[output_configs[0].topic], result)
    elif isinstance(result, dict):
        for topic_name, batch in result.items():
            cfg = outputs_by_topic.get(topic_name)
            if cfg is None:
                raise RuntimeError(
                    f"Handler returned output for topic {topic_name!r} "
                    "which is not in the node's output configuration"
                )
            _produce_one(producer, cfg, output_schemas[topic_name], batch)
    else:
        raise TypeError(f"Unexpected handler return type: {type(result)}")


def _produce_one(
    producer: KafkaProducer,
    cfg: OutputConfig,
    schema: pa.Schema,
    batch: pa.RecordBatch,
) -> None:
    if cfg.encoding == "arrow-batch":
        key = None
        if cfg.key:
            col = batch.column(batch.schema.get_field_index(cfg.key))
            key = str(col[0].as_py()).encode() if len(col) else None
        data = serialize_arrow(batch)
        producer.send(cfg.topic, value=data, key=key)
    else:
        messages = serialize_json_messages(batch)
        for i, msg in enumerate(messages):
            key = None
            if cfg.key:
                col = batch.column(batch.schema.get_field_index(cfg.key))
                val = col[i].as_py()
                key = str(val).encode() if val is not None else None
            producer.send(cfg.topic, value=msg, key=key)
    producer.flush()
    _RECORDS_PRODUCED.labels(topic=cfg.topic).inc(len(batch))


# ---------------------------------------------------------------------------
# Generator loop
# ---------------------------------------------------------------------------

def _run_generator(
    handler: Any,
    producer: KafkaProducer,
    node_config: NodeConfig,
    output_schemas: dict[str, pa.Schema],
    outputs_by_topic: dict[str, OutputConfig],
) -> None:
    interval_s = (node_config.tick_interval_ms or 1000) / 1000.0
    log.info("Generator tick interval: %.3fs", interval_s)
    while True:
        t0 = time.monotonic()
        try:
            result = handler.generate()
        except Exception:
            _HANDLER_ERRORS.inc()
            raise
        finally:
            _HANDLER_DURATION.observe(time.monotonic() - t0)

        if result is not None:
            _produce(producer, outputs_by_topic, output_schemas, result, node_config.outputs)

        elapsed = time.monotonic() - t0
        time.sleep(max(0.0, interval_s - elapsed))


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------

def run(node_config: NodeConfig, metrics_port: int = 9090) -> None:
    start_http_server(metrics_port)
    log.info("Prometheus metrics on :%d", metrics_port)

    handler = _load_handler(node_config.handler, node_config.config)
    log.info("Handler loaded: %s", node_config.handler)

    output_schemas = {out.topic: out.data_schema for out in node_config.outputs}
    outputs_by_topic: dict[str, OutputConfig] = {
        out.topic: out for out in node_config.outputs
    }

    producer: KafkaProducer | None = None
    if node_config.outputs:
        first_output = node_config.outputs[0]
        producer = KafkaProducer(bootstrap_servers=first_output.brokers)

    if isinstance(handler, ZeroToOne):
        if producer is None:
            raise RuntimeError("ZeroToOne handler requires at least one output")
        _run_generator(handler, producer, node_config, output_schemas, outputs_by_topic)
        return

    # Processor path (OneToOne / OneToMany / ManyToMany)
    if not node_config.inputs:
        raise RuntimeError(
            f"Handler {node_config.handler!r} is not a ZeroToOne generator "
            "but has no inputs configured"
        )

    input_schemas = {inp.topic: inp.data_schema for inp in node_config.inputs}
    input_by_topic: dict[str, InputConfig] = {inp.topic: inp for inp in node_config.inputs}

    first_input = node_config.inputs[0]
    consumer = KafkaConsumer(
        *[inp.topic for inp in node_config.inputs],
        bootstrap_servers=first_input.brokers,
        group_id=first_input.group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    buffers: dict[str, list] = defaultdict(list)
    buffer_start: dict[str, float] = {}

    log.info("Starting poll loop")
    while True:
        records = consumer.poll(timeout_ms=200)
        now = time.monotonic()

        for tp, msgs in records.items():
            topic = tp.topic
            if topic not in buffer_start:
                buffer_start[topic] = now
            buffers[topic].extend(msgs)
            _RECORDS_CONSUMED.labels(topic=topic).inc(len(msgs))

        for inp in node_config.inputs:
            topic = inp.topic
            buf = buffers[topic]
            if not buf:
                continue

            buffer_size = inp.buffer_size or 1000
            timeout_ms = inp.timeout_ms or 5000
            elapsed_ms = (now - buffer_start.get(topic, now)) * 1000

            if len(buf) >= buffer_size or elapsed_ms >= timeout_ms:
                _flush(
                    consumer=consumer,
                    producer=producer,
                    handler=handler,
                    topic=topic,
                    messages=buf,
                    input_schema=input_schemas[topic],
                    encoding=inp.encoding,
                    output_configs=node_config.outputs,
                    outputs_by_topic=outputs_by_topic,
                    output_schemas=output_schemas,
                )
                buffers[topic] = []
                buffer_start.pop(topic, None)


def _flush(
    consumer: KafkaConsumer,
    producer: KafkaProducer | None,
    handler: Any,
    topic: str,
    messages: list,
    input_schema: pa.Schema,
    encoding: str,
    output_configs: list[OutputConfig],
    outputs_by_topic: dict[str, OutputConfig],
    output_schemas: dict[str, pa.Schema],
) -> None:
    # Deserialise
    if encoding == "arrow-batch":
        # Each message is one RecordBatch; concatenate
        batches = [deserialize_arrow(m.value, input_schema) for m in messages]
        if len(batches) == 1:
            batch = batches[0]
        else:
            batch = pa.concat_batches(batches)
    else:
        batch = deserialize_json_messages([m.value for m in messages], input_schema)

    # Call handler
    t0 = time.monotonic()
    try:
        if isinstance(handler, ManyToMany):
            result = handler.process(batch, topic)
        else:
            result = handler.process(batch)
    except Exception:
        _HANDLER_ERRORS.inc()
        raise
    finally:
        _HANDLER_DURATION.observe(time.monotonic() - t0)

    # Produce output (must succeed before committing offsets)
    if result is not None and producer is not None:
        _produce(producer, outputs_by_topic, output_schemas, result, output_configs)

    # Commit offsets per partition
    offsets: dict[TopicPartition, Any] = {}
    for m in messages:
        tp = TopicPartition(m.topic, m.partition)
        if tp not in offsets or m.offset > offsets[tp].offset:
            from kafka.structs import OffsetAndMetadata
            offsets[tp] = OffsetAndMetadata(m.offset + 1, "", -1)
    consumer.commit(offsets=offsets)
