---
status: DRAFT
---

# End-to-End OpenTelemetry Distributed Tracing across tkati Pipelines

* **Author:** Andrey Tatarinov
* **Date:** 2026-11-10
* **Status:** DRAFT

## Context

`tkati` is a streaming dataflow framework operating on Apache Kafka and ClickHouse. Its execution engine processes batches of in-memory Apache Arrow tables (`pa.Table` / `RecordBatch`), backed by native PyO3 Rust extensions (`packages/tkati-core/src/`) for Kafka consumption and Rayon-parallelized JSON serialization. Worker nodes execute either synchronous loops (`SyncNode`) or overlapped asynchronous delivery loops (`PipelinedNode`) via `packages/tkati-core/tkati_core/node.py`.

Observability across `tkati` pipelines is currently limited to batch-level and loop-level phase metrics (`LoopStats`, Prometheus counters for `consumer/*`, `producer/*`, and loop phase latencies). There is no distributed tracing across pipeline topologies:
- When an upstream service produces an event into an ingest topic, its trace context terminates at the Kafka boundary.
- When messages traverse a multi-hop pipeline (e.g. `raw-events` -> `tkati-node-el` -> `dedup-events` -> `tkati-node-dedup` -> `clickhouse-sink`), operators cannot trace an individual message's path, diagnose end-to-end processing delays, or identify which node dropped an event.
- Standard OpenTelemetry instrumentation in Python assumes per-request or per-message procedural execution. Tracing every single message in `tkati` pipelines running at 100k+ rows/s would incur excessive CPU overhead, GIL contention, and network saturation.

This concept establishes distributed tracing using head-based stride sampling (targeting 0-4 sampled spans per batch), wire propagation via standard Kafka record headers, in-memory row correlation via a dedicated internal PyArrow column (`tkati_traceparent`), and strict upstream tag propagation through processing nodes to terminal sinks.

---

## Analysis

### 1. Contrarian & Scope Minimization (YAGNI)
- **Problem**: Adding per-row tracing to a vectorized batch pipeline risks defeating the performance purpose of using Apache Arrow and PyO3 native codecs.
- **Resolution**: Use sparse head sampling via stride (`trace_each_nth`). At 0 to 4 spans per batch (stride of 1:500 or 1:1,000), 99.8% of rows have null trace values. Null values in Arrow take 1 bit in the validity bitmap and 0 bytes in payload storage, keeping memory overhead under 5 KiB per batch.
- **Pass-through guarantee**: Intermediate nodes never sample independently; they strictly follow upstream tags.

### 2. Codebase Mechanics & Native PyO3 Boundaries
- **Rust Kafka Consumer (`packages/tkati-core/src/kafka.rs`)**: `NativeConsumer` currently discards record headers in `librdkafka::Message`. The Rust extension must extract the 55-byte W3C `traceparent` header (`00-{trace_id}-{span_id}-{flags}`) into a compact structure in `RawBatch`.
- **Zero-Copy Arrow Assembly**: When `KafkaConsumer.read_arrow()` parses NDJSON payloads into a `pa.Table`, it appends `tkati_traceparent` as a nullable Arrow `StringArray`. Appending a column to an Arrow table is a metadata-only pointer addition ($O(1)$).
- **Outbound Wire Serialization**: In `packages/tkati-core/tkati_core/kafka/producer.py`, `KafkaProducer.produce_arrow()` must project out `tkati_traceparent` before passing the table to Rust `encode_arrow()`. Passing the column into `encode_arrow()` would serialize internal tracing IDs into the wire JSON payload, breaking schema compatibility. Extracted headers are passed to `NativeProducer.enqueue(..., headers=...)`.

### 3. Asynchronous Lifecycle & Reality-First Span Settlement
- **Span Settlement at `node.done()`**: Rather than artificially coupling span lifetimes to asynchronous background delivery acknowledgments, each node traces execution truth directly. The `node_process` span starts when the batch is yielded and closes when processing completes at `node.done()`. If delivery fails downstream and the batch is rewound (`Consumer.rewind`), the subsequent retry accurately records a new processing span under the same trace ID.

### 4. Transform Contract & Schema Invariants
- **User Code Responsibility**: The harness does not attempt complex automatic stripping and re-attaching of `tkati_traceparent` around arbitrary user transform functions. Instead, pipeline developers are contracted to preserve `tkati_traceparent` in their projections and transforms if they want end-to-end tracing across that step.
- **Serialization Boundary Protection**: The framework strips `tkati_traceparent` at outgoing boundaries (`KafkaProducer` maps it to Kafka headers; `ClickhouseProducer` drops it before `insert_arrow()`).

### 5. Dropped Rows & Terminal Sinks
- **Deduplication Drops (`tkati-node-dedup`)**: When `batch.filter(keep_mask)` drops duplicate rows, surviving rows naturally keep their trace column. For discarded rows, `tkati-node-dedup` checks for non-null `tkati_traceparent` and emits a terminal span event (`span.add_event("dedup_dropped", ...)`) with status `OK` before closing the span, preventing traces from appearing stalled or lost in Jaeger/Tempo.
- **ClickHouse Terminal Sink**: `ClickhouseProducer` strips `tkati_traceparent` prior to `insert_arrow()` to avoid ClickHouse schema mismatch errors (`NO_SUCH_COLUMN_IN_TABLE`), then closes the terminal span with status `OK` upon server acknowledgment.
- **Dead Letter Queue (DLQ)**: Sampled rows rejected by ClickHouse and routed to DLQ record `span.set_status(StatusCode.ERROR)` with DLQ destination attributes.

---

## Goals vs. Non-Goals

### Goals:
* **Wire Propagation**: `NativeConsumer` and `NativeProducer` in `tkati_core._native` extract and inject W3C `traceparent` Kafka record headers without degrading base batch throughput by more than 3%.
* **In-Memory Arrow Correlation**: A nullable Arrow string column (`tkati_traceparent`) is attached to `pa.Table` batches in `KafkaConsumer.read_arrow()`.
* **Head-Based Stride Sampling**: Ingest nodes implement stride head sampling configured via `trace_each_nth: int` (targeting 0-4 spans per batch), while respecting upstream sampled flags (`flags & 0x01 == 1`) from external headers.
* **Downstream Tag Following**: Intermediate nodes (`tkati-node-el`, `tkati-node-dedup`) do not initiate independent sampling; they strictly follow existing non-null `tkati_traceparent` values and generate child processing spans.
* **Payload Stripping & Sinks**: Outbound serialization in `KafkaProducer` strips `tkati_traceparent` from data payloads to populate Kafka headers. `ClickhouseProducer` strips `tkati_traceparent` prior to `insert_arrow()` and closes terminal sink spans.
* **Explicit Dropped Row Visibility**: Filtering nodes (`tkati-node-dedup`) check dropped rows for sampled traces and emit a closing `dedup_dropped` span event.
* **Lifecycle Integration**: Spans close cleanly at `node.done()`. Retried batches produce distinct spans reflecting actual processing attempts.
* **Telemetry Export**: Uses standard OpenTelemetry Python SDK `BatchSpanProcessor` with OTLP exporter, configurable via environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`).

### Non-Goals:
* Tracing 100% of events on high-throughput streams (explicitly avoided to maintain throughput).
* Tracing arbitrary user functions outside the `SyncNode`/`PipelinedNode` harness.
* Automatic shielding/re-attaching of `tkati_traceparent` in user transform functions.
* Replacing Prometheus loop phase telemetry (`LoopStats`). Tracing supplements phase metrics with distributed causal path tracking.
* Distributed tracing for `consume_pylist()` (Python list-of-dicts format). Arrow-based processing is the primary production path; pylist will have basic header passthrough only.

---

## Alternatives Weighed

1. **Payload Column vs. Kafka Headers for Wire Carrier**:
   * *Rejected by author*: Injecting trace IDs into the JSON or Arrow payload wire format breaks schema validation across downstream consumers, requires modifying database schemas in ClickHouse, and contaminates business data contracts. Kafka record headers keep wire payloads clean.
2. **Per-Batch Tracing Only (No Row Tracking)**:
   * *Rejected by author*: Batch-only tracing fails to correlate specific business entities or records across multi-stage pipelines where batches are split, merged, or re-grouped.
3. **Tracing 100% of Events**:
   * *Rejected by author*: Python OpenTelemetry span object allocation and GIL overhead for 100,000+ events per second would reduce pipeline throughput by an order of magnitude.
4. **Harness Automatic Shielding of `tkati_traceparent` around Transforms**:
   * *Rejected by author*: Automatically stripping and re-joining columns around user transforms adds harness complexity and overhead. A clear contract requiring user code to preserve `tkati_traceparent` keeps the execution harness simple and explicit.
5. **Deferred Delivery Settlement for `PipelinedNode` Spans**:
   * *Rejected by author*: Tying processing span closure to asynchronous broker delivery acknowledgments obscures processing execution time and creates complex span holding state. Tracing actual processing completion at `node.done()` reflects reality directly.

---

## Key Decisions & Staging for Next Steps

1. **Native Header Extension (`packages/tkati-core/src/`)**: Extend `librdkafka` bindings to extract and produce record headers without per-message Python allocations.
2. **Arrow Column Protocol**: Designate `tkati_traceparent` as an internal column name; strip it automatically at serialization boundaries (`KafkaProducer` and `ClickhouseProducer`).
3. **Span Boundaries**: Close `node_process` spans synchronously at `node.done()`. Retried batches record new processing spans under the same trace ID.
4. **Dropped-Span Visibility**: Implement a dropped-row check in `tkati-node-dedup` to record `dedup_dropped` span events for sampled rows that are filtered out.
