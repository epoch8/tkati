"""End-to-end distributed tracing test for tkati-node-dedup."""

import pyarrow as pa
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode
from opentelemetry.util._once import Once
from tkati_core.settings import TracingSettings
from tkati_core.testing import memory_pipelined_node
from tkati_node_dedup.main import run
from tkati_node_dedup.store import BucketedDedupStore


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


def test_dedup_tracing_surviving_and_dropped_rows(tmp_path, span_exporter):
    store = BucketedDedupStore(
        root_dir=str(tmp_path / "dedup-store"),
        window_hours=1,
        bucket_hours=1,
    )

    tp_user1_first = "00-11111111111111111111111111111111-aaaaaaaaaaaaaaaa-01"
    tp_user1_dup = "00-22222222222222222222222222222222-bbbbbbbbbbbbbbbb-01"
    tp_user2_unique = "00-33333333333333333333333333333333-cccccccccccccccc-01"

    table = pa.table({
        "uid": ["user-1", "user-1", "user-2"],
        "tkati_traceparent": [tp_user1_first, tp_user1_dup, tp_user2_unique],
    })

    tracing = TracingSettings(enabled=True, service_name="tkati-dedup")
    node, _consumer, producer = memory_pipelined_node([table], tracing=tracing)
    assert producer is not None

    with node:
        run(node, store, field_name="uid")

    # Verify that producer received the deduplicated batch
    assert len(producer.sent) == 1
    produced_table = producer.sent[0]
    assert isinstance(produced_table, pa.Table)
    assert len(produced_table) == 2
    assert produced_table.column("uid").to_pylist() == ["user-1", "user-2"]
    # Verify surviving rows retain their tkati_traceparent
    assert "tkati_traceparent" in produced_table.schema.names
    assert produced_table.column("tkati_traceparent").to_pylist() == [
        tp_user1_first,
        tp_user2_unique,
    ]

    # Verify OpenTelemetry spans
    spans = span_exporter.get_finished_spans()
    assert len(spans) == 3

    span_by_trace = {format(s.context.trace_id, "032x"): s for s in spans}
    span_user1_first = span_by_trace["11111111111111111111111111111111"]
    span_user1_dup = span_by_trace["22222222222222222222222222222222"]
    span_user2 = span_by_trace["33333333333333333333333333333333"]

    # Surviving rows finish with OK and no drop events
    assert span_user1_first.status.status_code == StatusCode.OK
    assert len(span_user1_first.events) == 0

    assert span_user2.status.status_code == StatusCode.OK
    assert len(span_user2.events) == 0

    # Dropped duplicate row emits row_dropped event and finishes with OK
    assert span_user1_dup.status.status_code == StatusCode.OK
    assert len(span_user1_dup.events) == 1
    assert span_user1_dup.events[0].name == "row_dropped"
