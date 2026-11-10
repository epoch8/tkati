# Goldfish Review Report — Stage 8: Intermediate Zoom Out Review

**Target:** `design-docs/2026-11-10-e2e-tracing/README.md` (`1..381`)

## Reviewer Status

| Reviewer | Status |
| --- | --- |
| `egm-zoom-out` | completed |

## Prioritized Findings (Blockers -> Major -> Nits)

### 1. Centralize Span Lifecycle, Context Rewriting, and Drop Detection in `_NodeBase` - Severity: Blocker
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `## Critical User Journeys (CUJs)` (Lines 53–67), `## Technical Plan (Architecture)` (Lines 76–185), and `## Detailed Implementation Plan` (Steps 7, 9, 10, 11; Lines 301–338)
* **Discrepancy Description**:
  Across the Initial Zoom-Out, Comprehension, and Critic stages, the largest cluster of blockers stems from scattering trace responsibilities across `KafkaConsumer.read_arrow()`, `_NodeBase`, `ClickhouseProducer`, and `_dedupe_batch()` in `tkati-node-dedup`:
  1. **Orphaned Root Spans & Unrewritten Child Contexts**: Generating fake `traceparent` strings in `KafkaConsumer.read_arrow()` on the read-ahead thread creates an unexported root `span_id`, and never updating `tkati_traceparent` when `_NodeBase` starts a child span leaves downstream nodes with the old parent `span_id`.
  2. **Broken Dedup Span Lookup & PII Risk**: Tasking `_dedupe_batch()` in `tkati-node-dedup` with looking up spans in `node.py`, inverting masks, and attaching `{"dedup.key": key, "dedup.bucket": bucket_id}` leaks business keys (PII) into telemetry, requires invasive plumbing into a pure tabular helper, and leaves row filtering in `tkati-node-el` uninstrumented.
  3. **Duplicate Sink Span Management & DLQ Stripping Bug**: ClickHouse sinks are nodes configured with `ClickhouseProducer`. Because `ClickhouseProducer.produce_arrow()` executes synchronously inside `_NodeBase._take_batch()`, the node's processing span already wraps the ClickHouse insert. Having `ClickhouseProducer` also create/close spans duplicates `_NodeBase`'s responsibility, and stripping `tkati_traceparent` in `produce_arrow()` before `_insert_with_dlq_fallback()` breaks trace header propagation to the DLQ topic.
* **Rationale & Recommendation**:
  Consolidate all span lifecycle management into `_NodeBase` so that `KafkaConsumer`, `ClickhouseProducer`, `tkati-node-dedup`, and `tkati-node-el` remain trace-agnostic:
  - **In `NativeConsumer` / `KafkaConsumer.read_arrow()`**: Only extract incoming W3C `traceparent` headers whose sampled bit is set (`flags & 0x01 == 1`) into the nullable `tkati_traceparent` Arrow column (unsampled `flags=00` headers become `null`). Do not perform head sampling or span generation inside `KafkaConsumer`.
  - **In `_NodeBase._next_event()` (when yielding a `Batch[pa.Table]`)**:
    1. Apply stride head sampling (`trace_each_nth`) on untraced rows and enforce a per-batch cap (`max_spans_per_batch: int = 16`) across all sampled rows in the batch.
    2. For each sampled row, start an OpenTelemetry span (a root span if head-sampled at this node; a child span extracted from the incoming `tkati_traceparent` if sampled upstream).
    3. Format each started span's new W3C `traceparent` (`00-{trace_id}-{new_span_id}-01`), write those updated strings into the batch's `tkati_traceparent` column, and store the active spans in `self._active_spans: dict[str, Span]` keyed by the newly minted (batch-unique) `traceparent` string.
  - **In `_NodeBase._take_batch()` / `done()` / `__exit__()`**:
    1. Diff the keys of `self._active_spans` against the non-null `tkati_traceparent` values surviving in `output_arrow`. For any traceparent absent from `output_arrow`, record a `row_dropped` span event (without PII business keys), set `StatusCode.OK`, and end the span.
    2. When `done()` completes, set `StatusCode.OK` and end all remaining spans for that batch.
    3. In `_NodeBase.__exit__()`, if an uncommitted batch fails with an exception, set `StatusCode.ERROR`, record the exception, and end all uncommitted spans so none leak on rewind.
  - **Remove Step 11 (`tkati-node-dedup/src/tkati_node_dedup/main.py`)**: Because `_NodeBase` detects dropped rows generically via `event.data` vs. `output_arrow`, neither `tkati-node-dedup` nor `tkati-node-el` needs any code changes for drop observation.
  - **Simplify Step 9 (`ClickhouseProducer`)**: Strip `"tkati_traceparent"` exclusively inside `_insert_with_retry()` right before `ch_client.insert_arrow(...)`. Do not strip it in `produce_arrow()` (preserving it for `dlq_producer.produce_arrow(table)`), and do not add OpenTelemetry span logic to `ClickhouseProducer`.

### 2. Unbounded Upstream Sampling, Missing `Risks & Mitigations`, and Speculative Configuration (`YAGNI`) - Severity: Blocker
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `## Open Questions` (Lines 47–50), `#### 5. Configuration Model` (Lines 186–196), `## Non-Functional Requirements` (Lines 240–252), and `#### 6. settings.py` (Lines 290–300)
* **Discrepancy Description**:
  1. Accepting every upstream `traceparent` header without checking `flags & 0x01 == 1` or capping spans per batch allows an upstream producer to force per-row Python span creation on 100% of rows, violating the 3% throughput gate.
  2. Lines 47–50 ask questions already decided in the plan, `sample_upstream: bool = True` is speculative configuration bloat (`YAGNI`), `tracestate` is mentioned in Goals (line 30) and never implemented, and `TracingSettings` is described as a `dataclass` instead of `pydantic.BaseModel`.
* **Rationale & Recommendation**:
  - Add a concise `## Risks & Mitigations` section covering: (a) upstream sampling floods mitigated by filtering `flags & 0x01 == 1` and enforcing `max_spans_per_batch: int = 16`, (b) malformed headers ignored in Rust if not valid 55-byte ASCII W3C format, (c) zero PII leakage by never attaching payload columns/keys to span attributes, and (d) non-blocking `BatchSpanProcessor` drop-on-full behavior when the OTLP collector is unreachable.
  - Define `TracingSettings(BaseModel)` in `tkati_core/settings.py` with only `enabled: bool = False`, `trace_each_nth: int = 0`, and `max_spans_per_batch: int = 16`. Leave exporter endpoint and service name (`OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`) to standard OpenTelemetry SDK environment variables.
  - Remove `sample_upstream`, remove `(and optional tracestate)`, and resolve/remove `## Open Questions`.

### 3. Inconsistent PyO3 / Producer Boundary Signatures and Unhandled `"arrow-batch"` Mode - Severity: Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `### Component Details` (Lines 120–157) and `## Detailed Implementation Plan` (Steps 3, 4, 5, 7, 8; Lines 268–316)
* **Discrepancy Description**:
  1. The Architecture section calls `raw_batch.traceparents_arrow()` and proposes changing `EncodedBatch` to 3-tuples, whereas the Implementation Plan declares `raw_batch.traceparents() -> list[str | None]` and passes `headers: Sequence[str | None] | None = None` to `NativeProducer.enqueue(batch, tag=0, headers=None)`.
  2. `KafkaProducer.produce_arrow()` supports both `self.format == "json"` (N messages) and `self.format == "arrow-batch"` (1 Arrow IPC message). Stripping `tkati_traceparent` and passing N headers in `"arrow-batch"` mode breaks `enqueue` (`1 != N`) and loses the trace column inside the Arrow IPC payload.
* **Rationale & Recommendation**:
  - Standardize on `RawBatch.traceparents(self) -> list[str | None]` and `NativeProducer.enqueue(self, batch: EncodedBatch, tag: int = 0, headers: Sequence[str | None] | None = None) -> None`, leaving `EncodedBatch` and `encode_arrow` untouched.
  - Specify in `KafkaProducer.produce_arrow()` that `tkati_traceparent` is extracted into `headers` and dropped via `drop_columns(["tkati_traceparent"])` only when `self.format == "json"`; when `self.format == "arrow-batch"`, the column stays inside the serialized Arrow IPC stream and `headers=None` is passed to `enqueue`.

### 4. Repository Design Doc Structure (`AGENTS.md`), Altitude Control, and Calibrated Claims - Severity: Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** Full document structure and Lines 7–9, 134–185, 200–250
* **Discrepancy Description**:
  1. `AGENTS.md` requires top-level sections `## Context`, `## Goal` (containing `Done when:` and `Non-goals:` bullet lists), `## Approach`, `## Design`, and `## Implementation Steps`. Also, lines 206–236 include the redundant prefix `Rejected by author:`.
  2. Lines 145 and 244 omit the 32-bit Arrow StringArray offset buffer (`(N + 1) * 4` bytes = ~4.0 KiB per 1,000 rows) when explaining why the column takes ~4.2 KiB, and line 249 calls `tracing.enabled = False` "zero-overhead passthrough mode" when disabled mode is a no-op.
* **Rationale & Recommendation**:
  Align top-level headings with `AGENTS.md`, strip `Rejected by author:`, remove duplicate metadata bullets, include the 32-bit offset buffer in the memory calculation, and state that `tracing.enabled = False` is a no-op mode.
