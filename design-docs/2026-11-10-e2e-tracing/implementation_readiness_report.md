# Goldfish Review Report — Stage 9: Implementation Readiness Test

**Target:** `design-docs/2026-11-10-e2e-tracing/README.md` (`254..381`)

## Reviewer Status

| Reviewer | Status |
| --- | --- |
| `egm-implementation-readiness` | completed |
| `egm-codebase-details` | completed |
| `egm-style` | completed |

## Prioritized Findings (Blockers -> Major -> Nits)

### 1. Wrong Rust Struct Names and Method Signatures in `src/kafka.rs` vs `src/lib.rs` - Severity: Blocker
* **Reviewer Lens:** `egm-codebase-details`
* **Affected Scope:** Items 3, 4, 5 (Lines 268–288), `packages/tkati-core/src/kafka.rs`, `packages/tkati-core/src/lib.rs`
* **Discrepancy Description**:
  - Item 3 (lines 268–275) instructs modifying `NativeConsumer::poll_step`, `RawBatch`, and `NativeProducer::enqueue` inside `packages/tkati-core/src/kafka.rs`.
  - In the actual codebase, `kafka.rs` defines `KafkaConsumer` and `KafkaProducer`. `RawBatch`, `NativeConsumer`, and `NativeProducer` are PyO3 wrapper classes defined in `packages/tkati-core/src/lib.rs`.
  - `KafkaConsumer::poll_step` (`kafka.rs:242-249`) does not have access to `RawBatch`; its signature is `pub fn poll_step(&self, deadline: Instant, max_messages: usize, payloads: &mut Payloads, offsets: &mut Offsets, errors: &mut Vec<String>) -> Result<(), &'static str>`, and `NativeConsumer::poll_batch` (`lib.rs:249-266`) calls `poll_step` in a loop before constructing `RawBatch`.
* **Rationale & Recommendation**:
  Specify the exact structs and signatures in each file:
  - In `packages/tkati-core/src/kafka.rs`: add `traceparents: &mut Vec<Option<String>>` to `KafkaConsumer::poll_step` (using `rdkafka::message::{Header, Headers, Message, OwnedHeaders}` to inspect `msg.headers()`), and update `KafkaProducer::enqueue` to `pub fn enqueue(&self, messages: &Messages, tag: usize, traceparents: Option<&[Option<String>]>) -> Result<(), KafkaError>`.
  - In `packages/tkati-core/src/lib.rs`: add `traceparents: Vec<Option<String>>` to `RawBatch`, update `RawBatch::from_payloads` (`#[pyo3(signature = (payloads, traceparents=None))]`) for broker-free unit tests, expose `fn traceparents(&self) -> Vec<Option<String>>` on `RawBatch`, and update `NativeProducer::enqueue` to `#[pyo3(signature = (batch, tag=0, headers=None))]` raising `PyValueError` if `headers.len() != batch.len()`.

### 2. Missing Child Span Context Rewrite in `tkati_traceparent` Column - Severity: Blocker
* **Reviewer Lens:** `egm-implementation-readiness`, `egm-codebase-details`
* **Affected Scope:** Items 7, 8, 9, 10 (Lines 301–331)
* **Discrepancy Description**:
  In Item 7, `KafkaConsumer.read_arrow()` populates `tkati_traceparent` with the incoming wire `traceparent` header. In Item 10, `node.py` extracts the parent W3C context from `tkati_traceparent` and starts a child span (generating a new `span_id`). In Item 8, `KafkaProducer.produce_arrow()` extracts `data.column("tkati_traceparent").to_pylist()` and forwards those strings as outbound Kafka headers. Because nothing updates the `tkati_traceparent` column in `table` with the newly started child span's `traceparent` (`00-{trace_id}-{child_span_id}-01`), outbound Kafka messages carry the upstream parent's `span_id` instead of the current node's child `span_id`.
* **Rationale & Recommendation**:
  Specify in Item 10 that when `node.py` starts a child span for each sampled row, it serializes the child span's W3C `traceparent` and replaces the `tkati_traceparent` column in `table` (via `table.set_column(...)`) before yielding `Batch(data=table)`.

### 3. Missing Span Registry Access & Conflicting Attributes Between `node.py` and `tkati-node-dedup` - Severity: Blocker
* **Reviewer Lens:** `egm-implementation-readiness`, `egm-codebase-details`
* **Affected Scope:** Items 10, 11 (Lines 324–338), `packages/tkati-node-dedup/src/tkati_node_dedup/main.py`
* **Discrepancy Description**:
  1. `_dedupe_batch(batch: pa.Table, field_name: str, store: BucketedDedupStore, pending: set[bytes])` in `tkati-node-dedup/src/tkati_node_dedup/main.py:69-74` is a standalone function that does not take `node` as an argument, and `_NodeBase` exposes no public method or attribute to look up or close active spans.
  2. If `done()` closes all active spans for the batch (line 329), closing dropped spans inside `_dedupe_batch()` without popping them from `_NodeBase`'s active span map will double-end spans.
  3. The `dedup_dropped` event attributes conflict across the doc (`{"dedup.key": key, "dedup.bucket": bucket_id}` in line 66 vs. `{"dedup.reason": "seen_key"}` in line 181 vs. unspecified in line 336, and `BucketedDedupStore.filter_duplicates` does not return `bucket_id`).
* **Rationale & Recommendation**:
  Either detect dropped traceparents automatically inside `_NodeBase._take_batch()` by comparing the input batch's `tkati_traceparent` values against `output_arrow`'s surviving `tkati_traceparent` values, or define a concrete method on `_NodeBase` (`node.drop_span(traceparent: str, event_name: str, attributes: Mapping[str, Any] | None = None) -> None`) and specify the exact signature update to `_dedupe_batch()` and the exact attribute dictionary.

### 4. Missing Wiring from `NodeSettings.tracing` to `KafkaConsumer` and `ClickhouseProducer` - Severity: Blocker
* **Reviewer Lens:** `egm-implementation-readiness`, `egm-codebase-details`
* **Affected Scope:** Items 6, 7, 9, 10 (Lines 290–330), and omitted files `packages/tkati-core/tkati_core/consumer.py` & `packages/tkati-core/tkati_core/producer.py`
* **Discrepancy Description**:
  Item 6 places `TracingSettings` on `NodeSettings.tracing`. However, `_NodeBase.from_settings(settings)` (`node.py:259-261`) constructs the consumer and producer via `build_consumer(settings.input)` (`consumer.py:100`) and `build_producer(settings.output, dlq_producer=dlq)` (`producer.py:66`), which only receive `settings.input` and `settings.output`. Neither `KafkaConsumer.__init__` / `from_input_settings` / `build_consumer` nor `ClickhouseProducer.__init__` / `from_output_settings` / `build_producer` is updated in the plan to receive `TracingSettings`, and `tkati_core/consumer.py` and `tkati_core/producer.py` are missing from the Target Files list. Additionally, it is unspecified whether `KafkaConsumer.read_arrow()` appends `"tkati_traceparent"` when `tracing.enabled == False` (which would break existing unit tests asserting exact table schemas).
* **Rationale & Recommendation**:
  Specify the exact constructor and factory method signatures across `build_consumer`, `KafkaConsumer`, `build_producer`, `ClickhouseProducer`, and `_NodeBase.from_settings` (or perform head-sampling and span management inside `_NodeBase` so `Consumer`/`Producer` only pass through headers when present), add any modified base files to the Target Files list, and state whether `tkati_traceparent` is appended when `tracing.enabled == False`.

### 5. Self-Contradictory Stripping Logic in `ClickhouseProducer` Breaks DLQ Trace Routing - Severity: Blocker
* **Reviewer Lens:** `egm-codebase-details`
* **Affected Scope:** Item 9 (Lines 317–322), `packages/tkati-core/tkati_core/clickhouse/producer.py`
* **Discrepancy Description**:
  Item 9 instructs dropping `"tkati_traceparent"` in *both* `produce_arrow()` and `_insert_with_retry()`. In `ClickhouseProducer.produce_arrow()` (`clickhouse/producer.py:212-255`), if `"tkati_traceparent"` is dropped at the top of `produce_arrow()`, then when `_insert_with_retry()` fails and falls back to `_insert_with_dlq_fallback(table=data, ...)`, the table being recursively bisected via `_table_slices()` and sent to `dlq_producer.produce_arrow(table)` has already lost its `"tkati_traceparent"` column. Also, the table name attribute on `ClickhouseProducer` is `self._table` (`clickhouse/producer.py:185`), not `table_name`.
* **Rationale & Recommendation**:
  Drop `"tkati_traceparent"` only inside `_insert_with_retry()` right before `ch_client.insert_arrow()`, so `_insert_with_dlq_fallback()` preserves `"tkati_traceparent"` down to single-row isolation (`len(table) == 1`) and `dlq_producer.produce_arrow(table)` forwards the trace header to the DLQ topic.

### 6. `_NodeBase` Method Names in `tkati_core/node.py` Do Not Match Codebase - Severity: Major
* **Reviewer Lens:** `egm-codebase-details`
* **Affected Scope:** Item 10 (Lines 324–330), `packages/tkati-core/tkati_core/node.py`
* **Discrepancy Description**:
  - `consume_arrow()` (`node.py:361-364`) only delegates to `self._consume(self._consumer.read_arrow)`; `Batch` events are actually constructed in `_NodeBase._next_event()` (`node.py:480-537`).
  - `_NodeBase` has no `done()` method; `SyncNode.done()` (`node.py:705`) and `PipelinedNode.done()` (`node.py:823`) share `_NodeBase._take_batch()` (`node.py:559`).
  - On exception before or inside `done()`, `_NodeBase.__exit__()` (`node.py:301-359`) rewinds the batch; the plan does not specify recording the exception (`span.record_exception(exc)`, `span.set_status(StatusCode.ERROR)`) and ending open spans in `__exit__()`.
* **Rationale & Recommendation**:
  Specify starting spans in `_next_event()`, ending them in `_take_batch()`, and failing/closing open batch spans in `__exit__()` when an exception triggers rewind.

### 7. `settings.py` Uses Pydantic `BaseModel`, Not `dataclasses.field` - Severity: Major
* **Reviewer Lens:** `egm-codebase-details`
* **Affected Scope:** Item 6 (Lines 290–299), `packages/tkati-core/tkati_core/settings.py`
* **Discrepancy Description**:
  Item 6 specifies `tracing: TracingSettings = field(default_factory=TracingSettings)`, whereas `tkati_core/settings.py` uses `pydantic.BaseModel` (`MetricsSettings = MetricsSettings()`, `PipelineSettings = PipelineSettings()`).
* **Rationale & Recommendation**:
  Specify `class TracingSettings(BaseModel):` with `@field_validator("trace_each_nth")` rejecting `< 0`, and attach it to `NodeSettings` as `tracing: TracingSettings = TracingSettings()`.

### 8. Missing Edge Case Details in PyArrow Array Creation and `KafkaProducer.produce_arrow()` - Severity: Major
* **Reviewer Lens:** `egm-implementation-readiness`, `egm-codebase-details`
* **Affected Scope:** Items 7, 8 (Lines 301–316), `kafka/consumer.py`, `kafka/producer.py`
* **Discrepancy Description**:
  1. **PyArrow `NullArray` vs `StringArray` trap**: When a batch has 0 sampled rows, `traceparents` is `[None, None, ...]`. Calling `pa.array(traceparents)` without `type=pa.string()` creates a `pa.NullArray` (`null` type) instead of a `pa.StringArray` (`string` type).
  2. **`RecordBatch` vs `Table` and `"arrow-batch"` format**: In `KafkaProducer.produce_arrow(self, data: pa.Table | pa.RecordBatch, ...)`, `pa.RecordBatch` has `.drop_columns()`, not `.drop()`, and in `self.format == "arrow-batch"` mode, the entire table is serialized into a single 1-message `EncodedBatch`, so passing $N$ row headers to `self.producer.enqueue` mismatches `len(messages) == 1`. Also specify dropping `"tkati_traceparent"` before `_to_wire_table(data, self.wire_type_overrides)`.
* **Rationale & Recommendation**:
  Mandate `pa.array(traceparents, type=pa.string())` in `KafkaConsumer.read_arrow()`, and in `KafkaProducer.produce_arrow()` use `data.drop_columns(["tkati_traceparent"])` before `_to_wire_table` only in the `self.format == "json"` branch.

### 9. Unresolved Open Questions, Tracer Setup Details, and Missing Incremental Phasing - Severity: Major
* **Reviewer Lens:** `egm-implementation-readiness`, `egm-codebase-details`, `egm-style`
* **Affected Scope:** Lines 46–50, 254–381
* **Discrepancy Description**:
  - Open Questions 1 and 2 (lines 46–50) remain unresolved, violating the Zero-Decision Rule.
  - Item 10 does not specify how `TracerProvider`, `BatchSpanProcessor`, and `OTLPSpanExporter` are initialized when `opentelemetry-exporter-otlp-proto-grpc` is an optional dependency or when `_NodeBase` is constructed directly in tests with `InMemorySpanExporter`, and span naming conflicts between `f"{node_name} process"` (line 167) and `"node_process"` (line 328).
  - The 14 file modifications are presented as a flat list without incremental buildable/testable phases and checkpoints.
  - Lines 266 and 307 make uncalibrated "without additional overhead" and "zero-copy column linkage" claims (`pa.array(list[str | None])` copies string bytes into Arrow buffers; only `table.append_column` avoids copying existing payload columns).
* **Rationale & Recommendation**:
  Resolve Open Questions 1 and 2, specify exact OTel provider/exporter initialization and fallback behavior, unify span naming, calibrate the memory/copy claims on lines 266 and 307, and group the file changes into 3 incremental buildable/testable phases.

### 10. Boilerplate Repetition, Hedged Types, and Tool Attribution Footer - Severity: Nit
* **Reviewer Lens:** `egm-style`
* **Affected Scope:** Lines 258–358, 381
* **Discrepancy Description**:
  - Every file entry repeats a redundant `* **Type / Scope**:` bullet restating the filename.
  - Lines 265, 272, and 279 use hedged phrasing ("verify feature flags or standard header APIs", "optional byte slices or strings", "Python sequence or PyArrow-compatible array").
  - Line 381 contains the tool attribution footer `*Drafted with EGM*`.
* **Rationale & Recommendation**:
  Drop redundant `Type / Scope` bullets, commit to concrete types (`Vec<Option<String>>` and `list[str | None]`), and remove the `*Drafted with EGM*` footer.
