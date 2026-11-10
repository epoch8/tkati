"""Unit and integration tests for distributed tracing across tkati-core components."""

from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from clickhouse_connect.driver import exceptions as ch_exc
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode
from opentelemetry.util._once import Once
from tkati_core import Batch, SyncNode
from tkati_core._native import RawBatch
from tkati_core.clickhouse.producer import (
    _insert_with_dlq_fallback,
    _insert_with_retry,
)
from tkati_core.kafka.consumer import TRACEPARENT_COLUMN, KafkaConsumer
from tkati_core.kafka.producer import KafkaProducer
from tkati_core.settings import TracingSettings
from tkati_core.testing import MemoryConsumer, MemoryProducer


@pytest.fixture
def span_exporter():
    trace._TRACER_PROVIDER = None
    trace._TRACER_PROVIDER_SET_ONCE = Once()

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    yield exporter
    exporter.clear()
    trace._TRACER_PROVIDER = None
    trace._TRACER_PROVIDER_SET_ONCE = Once()


def test_raw_batch_traceparents():
    tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    batch = RawBatch.from_payloads([b'{"id": 1}', b'{"id": 2}'], [tp, None])
    assert batch.traceparents() == [tp, None]

    with pytest.raises(ValueError, match="mismatched traceparents length"):
        RawBatch.from_payloads([b'{"id": 1}'], [tp, None])


def test_kafka_consumer_read_arrow_attaches_traceparent():
    tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    raw_with_tp = RawBatch.from_payloads(
        [b'{"id": "1"}', b'{"id": "2"}'],
        [tp, None],
    )
    consumer = KafkaConsumer(
        kafka_config={"bootstrap.servers": "localhost:9092", "group.id": "test"},
        topic_name="test-topic",
        input_schema={"id": "string"},
    )
    consumer.consumer = MagicMock()
    consumer.consumer.poll_batch.return_value = raw_with_tp

    batch = consumer.read_arrow(timeout=1, num_messages=10)
    assert batch is not None
    table = batch.data
    assert TRACEPARENT_COLUMN in table.schema.names
    assert table.column(TRACEPARENT_COLUMN).to_pylist() == [tp, None]

    # When no sampled headers exist, column should not be added
    raw_no_tp = RawBatch.from_payloads([b'{"id": "1"}', b'{"id": "2"}'])
    consumer.consumer.poll_batch.return_value = raw_no_tp
    batch2 = consumer.read_arrow(timeout=1, num_messages=10)
    assert batch2 is not None
    assert TRACEPARENT_COLUMN not in batch2.data.schema.names


def test_kafka_producer_json_strips_traceparent_and_passes_headers():
    producer = KafkaProducer(
        kafka_config={"bootstrap.servers": "localhost:9092"},
        topic_name="test-topic",
        format="json",
        output_schema={"id": "string"},
    )
    producer.producer = MagicMock()

    tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    table = pa.table({"id": ["1", "2"], "tkati_traceparent": [tp, None]})

    producer.produce_arrow(table)

    assert producer.producer.enqueue.called
    args, kwargs = producer.producer.enqueue.call_args
    batch = args[0]
    assert kwargs.get("headers") == [tp, None]

    # Verify JSON payload does not contain tkati_traceparent
    payload0 = batch.payloads()[0]
    assert b"tkati_traceparent" not in payload0


def test_kafka_producer_arrow_batch_preserves_traceparent():
    producer = KafkaProducer(
        kafka_config={"bootstrap.servers": "localhost:9092"},
        topic_name="test-topic",
        format="arrow-batch",
    )
    producer.producer = MagicMock()

    tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    table = pa.table({"id": ["1"], "tkati_traceparent": [tp]})

    producer.produce_arrow(table)

    assert producer.producer.enqueue.called
    args, _ = producer.producer.enqueue.call_args
    batch = args[0]
    payload = batch.payloads()[0]

    reader = pa.ipc.open_stream(pa.py_buffer(payload))
    read_table = reader.read_all()
    assert "tkati_traceparent" in read_table.schema.names
    assert read_table.column("tkati_traceparent").to_pylist() == [tp]


def test_clickhouse_producer_strips_traceparent():
    ch_client = MagicMock()
    tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    table = pa.table({"id": ["1"], "tkati_traceparent": [tp]})

    _insert_with_retry(
        ch_client=ch_client,
        table="my_table",
        arrow_table=table,
        deduplication_token="token1",
    )

    assert ch_client.insert_arrow.called
    inserted_table = ch_client.insert_arrow.call_args[1]["arrow_table"]
    assert "tkati_traceparent" not in inserted_table.schema.names
    assert "id" in inserted_table.schema.names


def test_clickhouse_dlq_fallback_preserves_traceparent():
    ch_client = MagicMock()
    # Simulate data error on single row to trigger DLQ
    data_err = ch_exc.DatabaseError("bad data")
    data_err.code = 27  # CANNOT_PARSE_INPUT_ASSERTION_FAILED
    ch_client.insert_arrow.side_effect = data_err

    dlq_producer = MagicMock()
    tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    table = pa.table({"id": ["1"], "tkati_traceparent": [tp]})

    _insert_with_dlq_fallback(
        table=table,
        ch_client=ch_client,
        ch_table="my_table",
        dlq_producer=dlq_producer,
        split_factor=2,
        deduplication_token="token1",
    )

    assert dlq_producer.produce_arrow.called
    dlq_table = dlq_producer.produce_arrow.call_args[0][0]
    assert "tkati_traceparent" in dlq_table.schema.names
    assert dlq_table.column("tkati_traceparent").to_pylist() == [tp]


def test_node_stride_head_sampling_and_span_hierarchy(span_exporter):
    consumer = MemoryConsumer(
        [pa.table({"val": [1, 2, 3, 4]})],
    )
    producer = MemoryProducer()
    tracing = TracingSettings(
        enabled=True,
        service_name="node-ingest",
        trace_each_nth=2,
        max_spans_per_batch=10,
    )

    node = SyncNode(
        consumer=consumer,
        producer=producer,
        batch_size=10,
        batch_timeout_sec=0,
        tracing=tracing,
    )
    consumer.on_exhausted = node._end_of_input

    with node:
        for event in node.consume_arrow():
            if isinstance(event, Batch):
                # Verify that tkati_traceparent was attached and sampled on rows 0 and 2
                tps = event.data.column("tkati_traceparent").to_pylist()
                assert tps[0] is not None
                assert tps[1] is None
                assert tps[2] is not None
                assert tps[3] is None
                node.done(event, output_arrow=event.data)

    spans = span_exporter.get_finished_spans()
    # 2 ingest root spans + 2 process child spans = 4 spans
    assert len(spans) == 4

    ingest_spans = [s for s in spans if s.name == "node-ingest ingest"]
    process_spans = [s for s in spans if s.name == "node-ingest process"]
    assert len(ingest_spans) == 2
    assert len(process_spans) == 2

    # Verify that each process span has its corresponding ingest span as parent
    ingest_span_ids = {s.context.span_id for s in ingest_spans}
    for ps in process_spans:
        assert ps.parent is not None
        assert ps.parent.span_id in ingest_span_ids
        assert ps.status.status_code == StatusCode.OK


def test_consecutive_node_shares_root_parent(span_exporter):
    # Upstream node already sampled row 0
    upstream_tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    table = pa.table({
        "val": [10, 20],
        "tkati_traceparent": [upstream_tp, None],
    })

    consumer = MemoryConsumer([table])
    producer = MemoryProducer()
    tracing = TracingSettings(
        enabled=True,
        service_name="node-2",
        trace_each_nth=0,  # No new sampling on ingest
    )

    node = SyncNode(
        consumer=consumer,
        producer=producer,
        batch_size=10,
        batch_timeout_sec=0,
        tracing=tracing,
    )
    consumer.on_exhausted = node._end_of_input

    with node:
        for event in node.consume_arrow():
            if isinstance(event, Batch):
                node.done(event, output_arrow=event.data)

    spans = span_exporter.get_finished_spans()
    # No new ingest span; only 1 process span for upstream_tp
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "node-2 process"
    assert format(span.context.trace_id, "032x") == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert format(span.parent.span_id, "016x") == "00f067aa0ba902b7"


def test_node_circuit_breaker_max_spans_per_batch(span_exporter):
    # Batch has 5 sampled rows, but max_spans_per_batch is 2
    tps = [f"00-4bf92f3577b34da6a3ce929d0e0e473{i}-00f067aa0ba902b{i}-01" for i in range(5)]
    table = pa.table({"val": list(range(5)), "tkati_traceparent": tps})

    consumer = MemoryConsumer([table])
    producer = MemoryProducer()
    tracing = TracingSettings(
        enabled=True,
        service_name="capped-node",
        max_spans_per_batch=2,
    )

    node = SyncNode(
        consumer=consumer,
        producer=producer,
        batch_size=10,
        batch_timeout_sec=0,
        tracing=tracing,
    )
    consumer.on_exhausted = node._end_of_input

    with node:
        for event in node.consume_arrow():
            if isinstance(event, Batch):
                node.done(event, output_arrow=event.data)

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 2


def test_node_drop_observation(span_exporter):
    tp_survived = "00-11111111111111111111111111111111-2222222222222222-01"
    tp_dropped = "00-33333333333333333333333333333333-4444444444444444-01"
    table_in = pa.table({
        "val": [1, 2],
        "tkati_traceparent": [tp_survived, tp_dropped],
    })

    consumer = MemoryConsumer([table_in])
    producer = MemoryProducer()
    tracing = TracingSettings(enabled=True, service_name="filter-node")

    node = SyncNode(
        consumer=consumer,
        producer=producer,
        batch_size=10,
        batch_timeout_sec=0,
        tracing=tracing,
    )
    consumer.on_exhausted = node._end_of_input

    with node:
        for event in node.consume_arrow():
            if isinstance(event, Batch):
                # Filter out the second row
                output_table = event.data.slice(0, 1)
                node.done(event, output_arrow=output_table)

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 2

    span_by_trace = {format(s.context.trace_id, "032x"): s for s in spans}
    survived_span = span_by_trace["11111111111111111111111111111111"]
    dropped_span = span_by_trace["33333333333333333333333333333333"]

    assert survived_span.status.status_code == StatusCode.OK
    assert len(survived_span.events) == 0

    assert dropped_span.status.status_code == StatusCode.OK
    assert len(dropped_span.events) == 1
    assert dropped_span.events[0].name == "row_dropped"


def test_node_error_status_on_failure(span_exporter):
    tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    table = pa.table({"val": [1], "tkati_traceparent": [tp]})

    consumer = MemoryConsumer([table])
    producer = MemoryProducer()
    tracing = TracingSettings(enabled=True, service_name="failing-node")

    node = SyncNode(
        consumer=consumer,
        producer=producer,
        batch_size=10,
        batch_timeout_sec=0,
        tracing=tracing,
    )
    consumer.on_exhausted = node._end_of_input

    def _failing_run():
        with node:
            for event in node.consume_arrow():
                assert isinstance(event, Batch)
                raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        _failing_run()

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.status.status_code == StatusCode.ERROR
    assert "boom" in span.status.description
    assert any(e.name == "exception" for e in span.events)
