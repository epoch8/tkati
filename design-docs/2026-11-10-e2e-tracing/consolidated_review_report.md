# Consolidated Goldfish Review Report

**Target Document:** [`design-docs/2026-11-10-e2e-tracing/README.md`](file:///workspace/design-docs/2026-11-10-e2e-tracing/README.md) (`1..381`)  
**FeatureRange:** `1..253` | **DetailedRange:** `254..381`  
**Execution Mode:** Consolidated Report with All Review Rubrics (Stages 5, 6, 7, 8, 9)

---

## Stage & Reviewer Execution Summary

| Stage | Report Artifact | Reviewers | Status |
| :--- | :--- | :--- | :--- |
| **Stage 5: Initial Zoom Out** | [`zoom_out_report.md`](file:///workspace/design-docs/2026-11-10-e2e-tracing/zoom_out_report.md) | `egm-zoom-out`, `egm-inventive-catalyst` | completed |
| **Stage 6: Comprehension Test** | [`comprehension_report.md`](file:///workspace/design-docs/2026-11-10-e2e-tracing/comprehension_report.md) | `egm-comprehension` | completed |
| **Stage 7: Critic Review** | [`critic_report.md`](file:///workspace/design-docs/2026-11-10-e2e-tracing/critic_report.md) | `egm-design`, `egm-structural`, `egm-style` | completed |
| **Stage 8: Intermediate Zoom Out** | [`intermediate_zoom_out_report.md`](file:///workspace/design-docs/2026-11-10-e2e-tracing/intermediate_zoom_out_report.md) | `egm-zoom-out` | completed |
| **Stage 9: Implementation Readiness** | [`implementation_readiness_report.md`](file:///workspace/design-docs/2026-11-10-e2e-tracing/implementation_readiness_report.md) | `egm-implementation-readiness`, `egm-codebase-details`, `egm-style` | completed |

---

## Consolidated Action Items (Prioritized: 🔴 Blocker -> 🟡 Major -> 🟢 Nit)

### 🔴 Blockers

#### C-1. Broken W3C Parent-Child Propagation & Orphaned Root Spans Across Hops
* **Stages / Reviewers:** Stage 5 (`egm-zoom-out`, `egm-inventive-catalyst`), Stage 6 (`egm-comprehension`), Stage 7 (`egm-design`), Stage 8 (`egm-zoom-out`), Stage 9 (`egm-implementation-readiness`, `egm-codebase-details`)
* **Affected Scope:** Lines 53–62, 139–173, 301–331
* **Problem**:
  1. **Unrewritten child `span_id` on outbound messages**: When `_NodeBase` starts a child span (`tracer.start_span(...)`) for a sampled row, that span gets a new `span_id`, but the `tkati_traceparent` column in the Arrow table is never updated with the new `00-{trace_id}-{child_span_id}-01` string. When `KafkaProducer.produce_arrow()` extracts `tkati_traceparent`, it forwards the *upstream parent's* `traceparent` unchanged—causing all downstream nodes (`tkati-node-el`, `tkati-node-dedup`, `ClickHouse`) to attach spans as flat siblings under the root span instead of forming a parent-child chain.
  2. **Orphaned root `span_id` on head-sampled messages**: Fabricating a `traceparent` string with a random `span_id` inside `KafkaConsumer.read_arrow()` without starting/exporting a root span for that ID causes `_NodeBase` to create a child span pointing to a non-existent parent `span_id`.
* **Actionable Fix**:
  Centralize head sampling and span creation in `_NodeBase._next_event()`:
  - When a row has no incoming `tkati_traceparent` and hits the stride counter, start a **root span** (no parent context) in `_NodeBase._next_event()`.
  - When a row has an incoming `tkati_traceparent`, extract its parent context and start a **child span** in `_NodeBase._next_event()`.
  - Immediately format each started span's context into a new W3C `traceparent` string (`00-{trace_id}-{new_span_id}-01`) and replace the `tkati_traceparent` column in `Batch.data` (via `table.set_column(...)`) so downstream producers and terminal sinks automatically receive the current node's child `span_id`.

#### C-2. Split-Brain Drop Observation & Broken Span Lookup Between `_NodeBase` and `tkati-node-dedup`
* **Stages / Reviewers:** Stage 5 (`egm-zoom-out`, `egm-inventive-catalyst`), Stage 6 (`egm-comprehension`), Stage 7 (`egm-design`), Stage 8 (`egm-zoom-out`), Stage 9 (`egm-implementation-readiness`, `egm-codebase-details`)
* **Affected Scope:** Lines 63–66, 168–184, 324–338 (`packages/tkati-core/tkati_core/node.py` & `packages/tkati-node-dedup/src/tkati_node_dedup/main.py`)
* **Problem**:
  1. `_dedupe_batch(batch, field_name, store, pending)` in `tkati-node-dedup/src/tkati_node_dedup/main.py` is a standalone function with no reference to `node` or its active span map.
  2. Line 168 indexes active spans by integer row index (`dict[int, Span]`), whereas lines 178–180 filter the table (losing original row indices) and iterate over `traceparent` strings (`for tp in dropped_traces:`).
  3. Closing dropped spans in `_dedupe_batch()` while `node.done()` also closes all batch spans will double-end spans, and hardcoding drop logic in `tkati-node-dedup` leaves rows filtered in `tkati-node-el` uninstrumented.
  4. The `dedup_dropped` attributes conflict across the doc (`{"dedup.key": key, "dedup.bucket": bucket_id}` on line 66 vs. `{"dedup.reason": "seen_key"}` on line 181; `BucketedDedupStore.filter_duplicates` does not return `bucket_id`, and logging raw business keys risks PII leakage).
* **Actionable Fix**:
  Store active batch spans in `_NodeBase` in a `dict[str, Span]` keyed by the row's newly minted (and batch-unique) `tkati_traceparent` string. In `_NodeBase._take_batch(event, output_arrow, ...)`, compute the set difference between `self._active_spans.keys()` and the non-null `tkati_traceparent` strings surviving in `output_arrow`. For each dropped traceparent, pop the span, emit `span.add_event("row_dropped")` (or allow an optional `drop_span()` helper), set `StatusCode.OK`, and end the span centrally—eliminating the need to modify `_dedupe_batch` in `tkati-node-dedup/src/tkati_node_dedup/main.py`.

#### C-3. Stripping `tkati_traceparent` in `ClickhouseProducer.produce_arrow()` Breaks DLQ Trace Routing
* **Stages / Reviewers:** Stage 5 (`egm-zoom-out`, `egm-inventive-catalyst`), Stage 6 (`egm-comprehension`), Stage 7 (`egm-design`), Stage 8 (`egm-zoom-out`), Stage 9 (`egm-implementation-readiness`, `egm-codebase-details`)
* **Affected Scope:** Lines 94–97, 158–160, 317–323 (`packages/tkati-core/tkati_core/clickhouse/producer.py`)
* **Problem**:
  Item 9 instructs dropping `"tkati_traceparent"` in both `ClickhouseProducer.produce_arrow()` and `_insert_with_retry()`. In `clickhouse/producer.py`, if `"tkati_traceparent"` is stripped at the top of `produce_arrow()`, then when `_insert_with_retry()` fails with a data error and falls back to `_insert_with_dlq_fallback()`, the table being recursively bisected via `_table_slices()` and sent to `dlq_producer.produce_arrow(table)` has already lost its `"tkati_traceparent"` column. Neither the DLQ Kafka message nor error span tracking will retain the rejected row's trace context. Also, the table attribute on `ClickhouseProducer` is `self._table`, not `table_name`.
* **Actionable Fix**:
  Strip `"tkati_traceparent"` **exclusively** inside `ClickhouseProducer._insert_with_retry()` right before calling `ch_client.insert_arrow(...)`. Keep the column intact through `produce_arrow()` and `_insert_with_dlq_fallback()` so that rejected single rows passed to `dlq_producer.produce_arrow(table)` retain `tkati_traceparent` and have their `traceparent` header emitted to the DLQ Kafka topic.

#### C-4. Wrong Rust Struct Locations and Method Signatures in `src/kafka.rs` vs `src/lib.rs`
* **Stages / Reviewers:** Stage 6 (`egm-comprehension`), Stage 8 (`egm-zoom-out`), Stage 9 (`egm-implementation-readiness`, `egm-codebase-details`)
* **Affected Scope:** Lines 120–136, 268–288 (`packages/tkati-core/src/kafka.rs`, `packages/tkati-core/src/lib.rs`, `packages/tkati-core/tkati_core/_native.pyi`)
* **Problem**:
  1. Item 3 places `NativeConsumer::poll_step`, `RawBatch`, and `NativeProducer::enqueue` in `src/kafka.rs`. In the codebase, `src/kafka.rs` defines `KafkaConsumer` and `KafkaProducer`, whereas `RawBatch`, `NativeConsumer`, and `NativeProducer` are PyO3 classes in `src/lib.rs`.
  2. `KafkaConsumer::poll_step` (`src/kafka.rs:242`) does not have access to `RawBatch`; `NativeConsumer::poll_batch` (`src/lib.rs:249`) calls `poll_step` in a loop and constructs `RawBatch`.
  3. The document contradicts itself on the return type and signature: line 135 calls `raw_batch.traceparents_arrow()`, line 127 modifies `EncodedBatch` to 3-tuples, while lines 286–287 declare `RawBatch.traceparents(self) -> list[str | None]` and `NativeProducer.enqueue(self, batch: EncodedBatch, tag: int = 0, headers: Sequence[str | None] | None = None) -> None`.
* **Actionable Fix**:
  Standardize on the exact Rust and PyO3 signatures:
  - `src/kafka.rs`: Add `traceparents: &mut Vec<Option<String>>` to `KafkaConsumer::poll_step` (extracting valid 55-byte ASCII W3C `"traceparent"` headers where `flags & 0x01 == 1`), and update `KafkaProducer::enqueue` to accept `traceparents: Option<&[Option<String>]>`.
  - `src/lib.rs` & `_native.pyi`: Add `traceparents: Vec<Option<String>>` to `RawBatch`, update `RawBatch.from_payloads(payloads, traceparents=None)` for broker-free testing, expose `def traceparents(self) -> list[str | None]: ...`, and update `NativeProducer.enqueue(self, batch: EncodedBatch, tag: int = 0, headers: Sequence[str | None] | None = None) -> None` (raising `ValueError` if `len(headers) != len(batch)`).

#### C-5. Missing `Risks & Mitigations` Section & Unbounded Upstream Sampling (`flags=00` / Floods)
* **Stages / Reviewers:** Stage 6 (`egm-comprehension`), Stage 7 (`egm-design`, `egm-structural`), Stage 8 (`egm-zoom-out`)
* **Affected Scope:** Lines 120–169, 186–195, 240–253
* **Problem**:
  1. `feature-spec-standard.md` mandates a `## Risks & Mitigations` section.
  2. Extracting all incoming `"traceparent"` headers regardless of the W3C sampled flag (`flags == 00`) causes 100% of rows from gateways that emit unsampled headers to be non-null in `tkati_traceparent`, triggering per-row Python span creation on every message.
  3. If an upstream producer sets `flags == 01` on all messages, there is no per-batch cap to protect the 3% throughput gate.
* **Actionable Fix**:
  - Filter incoming `"traceparent"` headers by `flags & 0x01 == 1` during extraction so unsampled headers become `None` in `tkati_traceparent`.
  - Add `max_spans_per_batch: int = 16` to `TracingSettings` to cap the maximum number of spans created per batch.
  - Add a dedicated `## Risks & Mitigations` section covering upstream sampling floods, malformed W3C headers, PII avoidance (no business keys in span attributes), and non-blocking `BatchSpanProcessor` queue full drops.

#### C-6. Missing `TracingSettings` Wiring Between `_NodeBase`, `KafkaConsumer`, and `ClickhouseProducer`
* **Stages / Reviewers:** Stage 8 (`egm-zoom-out`), Stage 9 (`egm-implementation-readiness`, `egm-codebase-details`)
* **Affected Scope:** Lines 290–330 (`settings.py`, `kafka/consumer.py`, `clickhouse/producer.py`, `node.py`)
* **Problem**:
  `TracingSettings` is placed on `NodeSettings.tracing`, but `_NodeBase.from_settings(settings)` builds the consumer and producer via `build_consumer(settings.input)` and `build_producer(settings.output)`, which never receive `settings.tracing`. Furthermore, unconditionally appending `tkati_traceparent` in `KafkaConsumer.read_arrow()` when `tracing.enabled == False` will break existing unit tests that check exact output schemas of `KafkaConsumer.read_arrow()`.
* **Actionable Fix**:
  Keep `KafkaConsumer` and `ClickhouseProducer` independent of `TracingSettings`:
  - `KafkaConsumer.read_arrow()` only appends `"tkati_traceparent"` when `raw_batch.traceparents()` contains at least one non-null header (or when `_NodeBase._next_event()` attaches/updates `"tkati_traceparent"` when `self._tracing.enabled` is `True`).
  - `_NodeBase` owns `self._tracing: TracingSettings` (passed to `_NodeBase.__init__` and wired in `_NodeBase.from_settings`), performing stride head sampling and span lifecycle inside `_NodeBase._next_event()` and `_NodeBase._take_batch()`.

---

### 🟡 Major Findings

#### C-7. `_NodeBase` Method Names (`_next_event`, `_take_batch`, `__exit__`) and Span Leak on Rewind
* **Stages / Reviewers:** Stage 7 (`egm-design`), Stage 8 (`egm-zoom-out`), Stage 9 (`egm-codebase-details`)
* **Affected Scope:** Lines 158–173, 324–330 (`packages/tkati-core/tkati_core/node.py`)
* **Problem**:
  - In `tkati_core/node.py`, `consume_arrow()` simply calls `self._consume(self._consumer.read_arrow)`; `Batch` objects are constructed in `_NodeBase._next_event()`.
  - `_NodeBase` does not have a `done()` method; `SyncNode.done()` and `PipelinedNode.done()` both call `_NodeBase._take_batch()`.
  - If an exception occurs before or inside `done()`, `_NodeBase.__exit__()` rewinds the consumer offsets, leaving active batch spans unclosed without error status.
* **Actionable Fix**:
  Specify starting spans in `_NodeBase._next_event()`, settling dropped and surviving spans in `_NodeBase._take_batch()`, and in `_NodeBase.__exit__()` recording the exception (`span.record_exception(exc)`, `span.set_status(StatusCode.ERROR)`) and ending any uncommitted spans before rewind.

#### C-8. PyArrow `NullArray` Trap, `RecordBatch.drop_columns`, and `KafkaProducer` `"arrow-batch"` Format
* **Stages / Reviewers:** Stage 7 (`egm-design`), Stage 8 (`egm-zoom-out`), Stage 9 (`egm-implementation-readiness`, `egm-codebase-details`)
* **Affected Scope:** Lines 148–157, 304–316 (`kafka/consumer.py`, `kafka/producer.py`)
* **Problem**:
  1. Calling `pa.array(traceparents)` when all elements are `None` produces a `pa.NullArray` (`null` type) instead of `pa.StringArray` (`string` type).
  2. `KafkaProducer.produce_arrow(self, data: pa.Table | pa.RecordBatch, ...)` accepts `pa.RecordBatch` (which has `.drop_columns()`, not `.drop()`) and supports `self.format == "arrow-batch"` (which produces a single 1-message IPC stream, where passing $N$ row headers to `enqueue` fails with `1 != N`).
* **Actionable Fix**:
  Mandate `pa.array(traceparents, type=pa.string())` when constructing the column, use `data.drop_columns(["tkati_traceparent"])` before `_to_wire_table` in `KafkaProducer.produce_arrow()`, and extract/strip per-row headers only in the `self.format == "json"` branch (leaving `tkati_traceparent` in the IPC table and `headers=None` when `self.format == "arrow-batch"`).

#### C-9. `TracingSettings` Must Inherit from `pydantic.BaseModel` and Resolve Open Questions (`YAGNI`)
* **Stages / Reviewers:** Stage 5 (`egm-zoom-out`), Stage 6 (`egm-comprehension`), Stage 8 (`egm-zoom-out`), Stage 9 (`egm-implementation-readiness`, `egm-codebase-details`)
* **Affected Scope:** Lines 46–50, 186–195, 290–299 (`packages/tkati-core/tkati_core/settings.py`)
* **Problem**:
  - `tkati_core/settings.py` uses `pydantic.BaseModel` (`tracing: TracingSettings = TracingSettings()`), not `dataclasses.field(default_factory=...)`.
  - `Open Questions` 1 and 2 remain unresolved in the text, violating the Zero-Decision Rule.
  - `sample_upstream: bool = True` and `tracestate` (line 30) are unimplemented/speculative YAGNI items.
* **Actionable Fix**:
  Define `class TracingSettings(BaseModel):` with `enabled: bool = False`, `trace_each_nth: int = 0`, `max_spans_per_batch: int = 16`, and `@field_validator("trace_each_nth", "max_spans_per_batch")`. Remove `sample_upstream`, remove `(and optional tracestate)`, and delete the resolved `## Open Questions` section.

#### C-10. Repository Design Doc Conventions (`AGENTS.md`), Incremental Phasing, and Calibrated Claims
* **Stages / Reviewers:** Stage 5 (`egm-zoom-out`), Stage 7 (`egm-structural`, `egm-style`), Stage 8 (`egm-zoom-out`), Stage 9 (`egm-codebase-details`, `egm-style`)
* **Affected Scope:** Lines 1–75, 145, 200–250, 254–381
* **Problem**:
  1. `AGENTS.md` specifies top-level sections `## Context`, `## Goal` (`Done when:` / `Non-goals:`), `## Approach`, `## Design`, and `## Implementation Steps`.
  2. `implementation-readiness-standard.md` requires grouping implementation steps into logical, buildable/testable phases with per-phase verification checkpoints.
  3. Lines 145 and 244 omit the 32-bit Arrow offset buffer (`(N + 1) * 4` bytes = 4,004 bytes for 1,000 rows) when explaining the ~4.2 KiB memory overhead; line 249 mislabels disabled mode (`tracing.enabled = False`) as "zero-overhead passthrough mode" instead of no-op mode; lines 266 and 307 make uncalibrated "without additional overhead" / "zero-copy column linkage" claims.
  4. Lines 206–236 include the redundant `Rejected by author:` prefix.
* **Actionable Fix**:
  Align section headers with `AGENTS.md`, partition `Implementation Steps` into 3 buildable/testable phases (Phase 1: Rust/PyO3 headers; Phase 2: Python Kafka/ClickHouse column & header handling; Phase 3: `_NodeBase` span lifecycle & tests), include the 32-bit offset buffer in the memory math, clarify no-op mode when `tracing.enabled = False`, and remove `Rejected by author:`.

---

### 🟢 Nits

#### C-11. Boilerplate File Entry Bullets, Hedged Types, Duplicate Metadata, and Attribution Footer
* **Stages / Reviewers:** Stage 5 (`egm-zoom-out`, `egm-inventive-catalyst`), Stage 6 (`egm-comprehension`), Stage 7 (`egm-design`, `egm-structural`, `egm-style`), Stage 9 (`egm-style`)
* **Affected Scope:** Lines 7–11, 37, 55, 129, 142, 224, 258–358, 381
* **Problem**:
  - Duplicate `* **Status:** Draft` bullet under H1 title (already in YAML frontmatter) and 80-character hyphen dividers.
  - Undefined `tkati-sink-ch` name in CUJ 1 (line 55) and undefined `global_offset` in line 142 (should be consumer/node monotonic row counter `self._rows_seen`).
  - Redundant `* **Type / Scope**:` bullets on every file in the implementation plan, hedged phrasing ("optional byte slices or strings", "Python sequence or PyArrow-compatible array"), abstract jargon ("execution truth", "leverages Arrow's vectorized SIMD operations"), and the `*Drafted with EGM*` footer on line 381.
* **Actionable Fix**:
  Clean up the metadata header, replace `global_offset` with `self._rows_seen`, drop redundant `Type / Scope` bullets, commit to concrete types (`Vec<Option<String>>` and `list[str | None]`), and remove `*Drafted with EGM*`.

---

## Cross-Stage Traceability Matrix

| Consolidated ID | Severity | Stage 5 (`zoom_out_report.md`) | Stage 6 (`comprehension_report.md`) | Stage 7 (`critic_report.md`) | Stage 8 (`intermediate_zoom_out_report.md`) | Stage 9 (`implementation_readiness_report.md`) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **C-1** (W3C Child Span Rewrite & Root Span) | 🔴 Blocker | Finding #1 | Finding #1 | Finding #3 | Finding #1.1 | Finding #2 |
| **C-2** (Centralized Drop Detection vs `tkati-node-dedup`) | 🔴 Blocker | Finding #1 | Finding #2 | Finding #4.3 | Finding #1.2 | Finding #3 |
| **C-3** (`ClickhouseProducer` DLQ Stripping Boundary) | 🔴 Blocker | Finding #2 | Finding #6 | Finding #4.2 | Finding #1.3 | Finding #5 |
| **C-4** (Rust `kafka.rs` vs `lib.rs` Signatures & Types) | 🔴 Blocker | — | Finding #4 | — | Finding #3.1 | Finding #1 |
| **C-5** (`Risks & Mitigations`, `flags=00`, & Per-Batch Cap) | 🔴 Blocker | — | Finding #5 | Findings #1, #2 | Finding #2.1 | — |
| **C-6** (`TracingSettings` Wiring & Disabled Schema Safety) | 🔴 Blocker | Finding #1.3 | — | — | Finding #1 | Finding #4 |
| **C-7** (`_NodeBase` `_next_event`/`_take_batch`/`__exit__`) | 🟡 Major | — | — | Finding #4.1 | Finding #1 | Finding #6 |
| **C-8** (`pa.string()`, `drop_columns`, & `"arrow-batch"` Mode) | 🟡 Major | — | — | Finding #5 | Finding #3.2 | Finding #8 |
| **C-9** (Pydantic `BaseModel`, YAGNI Settings, Open Questions) | 🟡 Major | Finding #4 | Findings #3, #5 | Finding #8 | Finding #2.2 | Findings #7, #9 |
| **C-10** (`AGENTS.md` Headings, Phasing, & Memory Math) | 🟡 Major | Finding #3 | — | Findings #6, #7 | Finding #4 | Finding #9 |
| **C-11** (Style, Terminology, Boilerplate, & Footer Nits) | 🟢 Nit | Finding #5 | Findings #6, #7 | Finding #8 | — | Finding #10 |
