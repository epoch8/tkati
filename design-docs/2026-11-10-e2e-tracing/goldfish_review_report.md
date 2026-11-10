# Goldfish Annotated Review Report

**Status:** resolved
**Target:** `design-docs/2026-11-10-e2e-tracing/README.md` (`1..381`)

## Prioritized Findings & AI Suggestions

### 1. Root Span Creation on Head Sampling & Shared Root Parent Model [`egm-design`, `egm-zoom-out`, `egm-comprehension`, `egm-implementation-readiness`, `egm-codebase-details`] - Severity: Blocker
* **Affected Scope:** [Lines 53-62](README.md#L53-L62), [Lines 139-173](README.md#L139-L173), [Lines 301-331](README.md#L301-L331) (`## Critical User Journeys`, `## Technical Plan`, `## Detailed Implementation Plan`)
* **Discrepancy Description:** On head-sampled messages, fabricating a `00-{trace_id}-{span_id}-01` string without physically creating and exporting a root span with `{span_id}` causes all node spans to point to a missing root span in Jaeger/Tempo. Meanwhile, reviewers flagged that `tkati_traceparent` is not rewritten at each hop.
* **Rationale:** Every `parent_span_id` referenced by child spans must exist as an exported span, while keeping `tkati_traceparent` immutable across hops (all consecutive node spans sharing the root parent) avoids rewriting the Arrow column at every node.
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Per author alignment, consecutive node spans will intentionally share the root parent (`tkati_traceparent` remains immutable across hops), and when an ingest node head-samples an untraced row via `trace_each_nth`, it physically creates and exports the root span (`f"{service_name} ingest"`) whose `00-{trace_id}-{root_span_id}-01` populates `tkati_traceparent`.
* ***Proposed Edit:***
  * Location: `CUJ 1`, `Component Details -> 2 & 4`, and `Detailed Implementation Plan -> Items 7 & 10`
  * Change: Specify that when an ingest node head-samples an untraced message, it starts and ends a physical root span (`f"{service_name} ingest"`) and writes that root span's W3C `traceparent` (`00-{trace_id}-{root_span_id}-01`) into `tkati_traceparent`. Downstream nodes keep `tkati_traceparent` immutable, creating child processing spans (`f"{service_name} process"`) that all share the root span as their parent, ordered chronologically on the trace timeline.

---

### 2. Centralize Drop Observation & Active Span Lookup in `_NodeBase` [`egm-zoom-out`, `egm-inventive-catalyst`, `egm-comprehension`, `egm-design`, `egm-implementation-readiness`, `egm-codebase-details`] - Severity: Blocker
* **Affected Scope:** [Lines 63-66](README.md#L63-L66), [Lines 168-184](README.md#L168-L184), [Lines 324-338](README.md#L324-L338) (`CUJ 3`, `4. Node Lifecycle & Span Management`, `Items 10 & 11`)
* **Discrepancy Description:** `_dedupe_batch(batch, field_name, store, pending)` in `tkati-node-dedup/src/tkati_node_dedup/main.py` is a standalone function with no reference to `node` or its active span map. Furthermore, indexing spans by integer row index (`dict[int, Span]`) breaks after `table.filter(...)`, closing spans in both `_dedupe_batch()` and `node.done()` double-ends spans, and hardcoding drop logic in `tkati-node-dedup` leaves row filtering in `tkati-node-el` uninstrumented.
* **Rationale:** `_NodeBase._take_batch()` already receives both the input `event.data` and the filtered `output_arrow` table.
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Storing active batch spans in `_NodeBase` as `self._active_spans: dict[str, list[Span]]` keyed by `tkati_traceparent` string and comparing input vs. `output_arrow` `tkati_traceparent` counts in `_NodeBase._take_batch()` automatically detects dropped/filtered rows across both `tkati-node-dedup` and `tkati-node-el`, emits `span.add_event("row_dropped")`, sets `StatusCode.OK`, and closes the span without touching `_dedupe_batch()` in `tkati-node-dedup/src/tkati_node_dedup/main.py` or leaking PII business keys into span attributes.
* ***Proposed Edit:***
  * Location: `CUJ 3`, `4. Node Lifecycle & Span Management`, and `Detailed Implementation Plan -> Items 10 & 11`
  * Change: Store active batch spans in `_NodeBase` keyed by `tkati_traceparent` string. In `_NodeBase._take_batch()`, identify any sampled row present in `event.data` that is absent from `output_arrow`, emit `span.add_event("row_dropped")`, set `StatusCode.OK`, and end the span. Remove `packages/tkati-node-dedup/src/tkati_node_dedup/main.py` from Target Files (keeping `test_tracing_dedup.py` to test drop event emission through the harness).

---

### 3. Strip `tkati_traceparent` in `ClickhouseProducer` Only Inside `_insert_with_retry()` [`egm-zoom-out`, `egm-inventive-catalyst`, `egm-design`, `egm-implementation-readiness`, `egm-codebase-details`] - Severity: Blocker
* **Affected Scope:** [Lines 94-97](README.md#L94-L97), [Lines 158-160](README.md#L158-L160), [Lines 317-323](README.md#L317-L323) (`Mermaid Diagram`, `3. Outbound Serialization Boundaries`, `Item 9`)
* **Discrepancy Description:** Dropping `"tkati_traceparent"` at the top of `ClickhouseProducer.produce_arrow()` removes the column before `_insert_with_dlq_fallback()` splits failed batches via `_table_slices()` and passes rejected rows to `dlq_producer.produce_arrow(table)`, losing the trace header on DLQ messages. Also, `ClickhouseProducer` uses `self._table` (not `table_name`) and executes synchronously inside `node.done()`.
* **Rationale:** Preserving `tkati_traceparent` through `_insert_with_dlq_fallback()` allows `dlq_producer` (a `KafkaProducer`) to automatically extract `tkati_traceparent` into the DLQ Kafka record header.
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Stripping `"tkati_traceparent"` exclusively inside `ClickhouseProducer._insert_with_retry()` right before `self.client.insert_arrow(...)` prevents ClickHouse `NO_SUCH_COLUMN_IN_TABLE` errors while keeping `tkati_traceparent` intact during `_insert_with_dlq_fallback()` recursive bisection and DLQ routing.
* ***Proposed Edit:***
  * Location: `Mermaid Diagram`, `3. Outbound Serialization Boundaries`, and `Item 9 (clickhouse/producer.py)`
  * Change: Specify that `ClickhouseProducer` strips `"tkati_traceparent"` (via `arrow_table.drop_columns(["tkati_traceparent"])`) exclusively inside `_insert_with_retry()` immediately before `self.client.insert_arrow()`, preserving the column across `_insert_with_dlq_fallback()` so rejected rows sent to `dlq_producer.produce_arrow(table)` retain their W3C `traceparent` header.

---

### 4. Exact Rust Struct Locations & PyO3 Method Signatures in `src/kafka.rs`, `src/lib.rs`, and `_native.pyi` [`egm-comprehension`, `egm-zoom-out`, `egm-implementation-readiness`, `egm-codebase-details`] - Severity: Blocker
* **Affected Scope:** [Lines 120-136](README.md#L120-L136), [Lines 268-288](README.md#L268-L288) (`1. PyO3 Native Extension`, `Items 3, 4, 5`)
* **Discrepancy Description:** Item 3 places `NativeConsumer::poll_step`, `RawBatch`, and `NativeProducer::enqueue` in `src/kafka.rs`, whereas `src/kafka.rs` defines `KafkaConsumer` and `KafkaProducer` and `src/lib.rs` defines the PyO3 wrappers `RawBatch`, `NativeConsumer`, and `NativeProducer`. Furthermore, line 135 calls `raw_batch.traceparents_arrow()` and line 127 modifies `EncodedBatch`, contradicting lines 286–287 (`RawBatch.traceparents() -> list[str | None]` and `NativeProducer.enqueue(batch, tag=0, headers=None)`).
* **Rationale:** Implementation instructions must match actual Rust module boundaries and expose a single consistent API across the document.
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Updating Items 1, 2, 3, 4, and 5 to match the exact structs in `src/kafka.rs` vs `src/lib.rs` and standardizing on `RawBatch.traceparents(self) -> list[str | None]` and `NativeProducer.enqueue(self, batch: EncodedBatch, tag: int = 0, headers: Sequence[str | None] | None = None) -> None` eliminates all cross-section contradictions.
* ***Proposed Edit:***
  * Location: `1. PyO3 Native Extension`, `2. Vectorized In-Memory Representation`, and `Items 3, 4, 5`
  * Change:
    - In `packages/tkati-core/src/kafka.rs`: Update `KafkaConsumer::poll_step` to accept `traceparents: &mut Vec<Option<String>>` (inspecting `msg.headers()` via `rdkafka::message::{Header, Headers, Message, OwnedHeaders}` and extracting valid 55-byte ASCII `"traceparent"` headers where `flags & 0x01 == 1`). Update `KafkaProducer::enqueue` to accept `traceparents: Option<&[Option<String>]>`.
    - In `packages/tkati-core/src/lib.rs` & `_native.pyi`: Add `traceparents: Vec<Option<String>>` to `RawBatch`, update `RawBatch.from_payloads(payloads, traceparents=None)`, expose `def traceparents(self) -> list[str | None]: ...`, and update `NativeProducer.enqueue(self, batch: EncodedBatch, tag: int = 0, headers: Sequence[str | None] | None = None) -> None` (raising `PyValueError` if `len(headers) != len(batch)`).

---

### 5. Add `Risks & Mitigations` Section, Filter Unsampled Headers (`flags=00`), and Cap Spans per Batch [`egm-design`, `egm-structural`, `egm-zoom-out`, `egm-comprehension`] - Severity: Blocker
* **Affected Scope:** [Lines 120-169](README.md#L120-L169), [Lines 186-195](README.md#L186-L195), [Lines 240-253](README.md#L240-L253)
* **Discrepancy Description:** `feature-spec-standard.md` requires a `## Risks & Mitigations` section. In addition, extracting incoming `"traceparent"` headers whose sampled bit is `00` (`flags & 0x01 == 0`) or allowing unbounded upstream `-01` headers would trigger Python span creation on up to 100% of rows in a batch, violating the 3% throughput gate.
* **Rationale:** Ignoring unsampled headers (`flags & 0x01 == 0`) and enforcing `max_spans_per_batch: int = 16` bounds worst-case tracing overhead even under upstream sampling storms.
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Filtering `flags & 0x01 == 1` in `KafkaConsumer::poll_step`, adding `max_spans_per_batch: int = 16` to `TracingSettings`, and adding a dedicated `## Risks & Mitigations` section resolves both the standard compliance blocker and the DoS vector.
* ***Proposed Edit:***
  * Location: `Component Details`, `TracingSettings`, and `## Risks & Mitigations`
  * Change: Specify that `KafkaConsumer::poll_step` only extracts `"traceparent"` headers that are 55 ASCII bytes with `flags & 0x01 == 1` (treating `00` or malformed headers as `None`), add `max_spans_per_batch: int = 16` to `TracingSettings`, and add a `## Risks & Mitigations` section covering upstream sampling storms, malformed W3C headers, PII avoidance, and non-blocking `BatchSpanProcessor` queue drops.

---

### 6. Wire `TracingSettings` Cleanly and Preserve Schema When Tracing Is Disabled [`egm-implementation-readiness`, `egm-codebase-details`, `egm-zoom-out`] - Severity: Blocker
* **Affected Scope:** [Lines 290-330](README.md#L290-L330) (`Items 6, 7, 10`)
* **Discrepancy Description:** `NodeSettings.tracing` is never passed into `build_consumer(settings.input)` or `KafkaConsumer`, and unconditionally appending `tkati_traceparent` in `KafkaConsumer.read_arrow()` when `tracing.enabled == False` and no headers exist would break existing unit tests asserting exact table schemas on `KafkaConsumer.read_arrow()`.
* **Rationale:** Keeping `TracingSettings` owned by `_NodeBase` avoids changing `build_consumer` / `build_producer` signatures, and `KafkaConsumer.read_arrow()` should only append `tkati_traceparent` when `any(raw_batch.traceparents())` is true (or `_NodeBase._next_event()` attaches it when head-sampling).
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Having `KafkaConsumer.read_arrow()` append `tkati_traceparent` only when `raw_batch.traceparents()` contains at least one non-null header (preserving existing test schemas when no trace headers are present) and performing stride head-sampling inside `_NodeBase._next_event()` (which directly owns `self._tracing: TracingSettings`) eliminates the missing consumer wiring and keeps `tkati_core/consumer.py` untouched.
* ***Proposed Edit:***
  * Location: `Component Details -> 2 & 4` and `Detailed Implementation Plan -> Items 7 & 10`
  * Change: Specify that `KafkaConsumer.read_arrow()` appends `"tkati_traceparent"` (`pa.array(tps, type=pa.string())`) when `any(tps)` is true; otherwise returns `table` unmodified. `_NodeBase.__init__` accepts `tracing: TracingSettings = TracingSettings()` (wired from `settings.tracing` in `_NodeBase.from_settings`), and `_NodeBase._next_event()` performs stride head-sampling (`trace_each_nth`), starts the physical root `ingest` span for newly sampled rows, appends/updates `"tkati_traceparent"` on the `pa.Table`, and starts the node's `process` child spans.

---

### 7. Match Actual `_NodeBase` Methods (`_next_event`, `_take_batch`, `__exit__`) & Close Spans on Exception [`egm-design`, `egm-codebase-details`, `egm-zoom-out`] - Severity: Major
* **Affected Scope:** [Lines 158-173](README.md#L158-L173), [Lines 324-330](README.md#L324-L330) (`4. Node Lifecycle & Span Management`, `Item 10`)
* **Discrepancy Description:** In `packages/tkati-core/tkati_core/node.py`, `consume_arrow()` delegates to `_consume()`, `Batch` events are constructed in `_NodeBase._next_event()`, `SyncNode.done()` and `PipelinedNode.done()` share `_NodeBase._take_batch()`, and `_NodeBase.__exit__()` rewinds uncommitted batches on exception.
* **Rationale:** Implementation steps must reference the actual methods in `node.py` and ensure active spans are marked `StatusCode.ERROR` and ended in `__exit__()` when an exception occurs before `done()`.
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Referencing `_NodeBase._next_event()`, `_NodeBase._take_batch()`, and `_NodeBase.__exit__()` makes Item 10 directly actionable and prevents span leaks on batch rewind.
* ***Proposed Edit:***
  * Location: `4. Node Lifecycle & Span Management` and `Item 10 (node.py)`
  * Change: Specify starting spans in `_NodeBase._next_event()`, ending dropped and completed spans in `_NodeBase._take_batch()`, and in `_NodeBase.__exit__()` recording the exception (`span.record_exception(exc)`, `span.set_status(StatusCode.ERROR)`) and ending any uncommitted batch spans when an exception triggers rewind.

---

### 8. Handle `pa.string()` Type, `RecordBatch.drop_columns`, and `KafkaProducer` `"arrow-batch"` Mode [`egm-design`, `egm-implementation-readiness`, `egm-codebase-details`] - Severity: Major
* **Affected Scope:** [Lines 148-157](README.md#L148-L157), [Lines 304-316](README.md#L304-L316) (`Items 7 & 8`)
* **Discrepancy Description:** Calling `pa.array(traceparents)` without `type=pa.string()` produces a `NullArray` when all elements are `None`; `pa.RecordBatch` uses `.drop_columns()` rather than `.drop()`; and in `KafkaProducer.produce_arrow()` when `self.format == "arrow-batch"`, the table is serialized into 1 IPC message (`len(messages) == 1`), so passing $N$ row headers to `enqueue` fails.
* **Rationale:** Both `"json"` and `"arrow-batch"` formats and both `pa.Table` and `pa.RecordBatch` inputs must work without type or length errors.
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Explicitly specifying `pa.array(..., type=pa.string())`, `data.drop_columns(["tkati_traceparent"])` before `_to_wire_table` in `self.format == "json"` mode, and keeping `"tkati_traceparent"` inside the Arrow IPC stream (with `headers=None`) in `self.format == "arrow-batch"` mode resolves all edge cases.
* ***Proposed Edit:***
  * Location: `3. Outbound Serialization Boundaries` and `Items 7 & 8`
  * Change: Specify `pa.array(traceparents, type=pa.string())`, use `data.drop_columns(["tkati_traceparent"])` prior to `_to_wire_table(data, self.wire_type_overrides)` in `KafkaProducer.produce_arrow()` when `self.format == "json"`, and leave `tkati_traceparent` inside the IPC stream (`headers=None`) when `self.format == "arrow-batch"`.

---

### 9. Use Pydantic `BaseModel` for `TracingSettings`, Remove YAGNI Knobs, and Resolve Open Questions [`egm-zoom-out`, `egm-comprehension`, `egm-implementation-readiness`, `egm-codebase-details`] - Severity: Major
* **Affected Scope:** [Lines 46-50](README.md#L46-L50), [Lines 186-195](README.md#L186-L195), [Lines 290-299](README.md#L290-L299) (`Open Questions`, `5. Configuration Model`, `Item 6`)
* **Discrepancy Description:** `tkati_core/settings.py` uses `pydantic.BaseModel`, not `dataclasses.field(default_factory=...)`; `Open Questions` 1 and 2 contradict settled sections; and `sample_upstream` and `tracestate` are unimplemented YAGNI items.
* **Rationale:** Aligning `TracingSettings` with `PipelineSettings(BaseModel)` in `settings.py` and removing unresolved open questions satisfies the Zero-Decision Rule.
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Defining `class TracingSettings(BaseModel):` with `enabled: bool = False`, `service_name: str | None = None`, `trace_each_nth: int = 0`, and `max_spans_per_batch: int = 16` (validated with `@field_validator`), removing `sample_upstream` and `(and optional tracestate)`, and removing the resolved `## Open Questions` section makes configuration unambiguous.
* ***Proposed Edit:***
  * Location: `Goals`, `Open Questions`, `5. Configuration Model`, and `Item 6 (settings.py)`
  * Change: Remove `## Open Questions`, remove `(and optional tracestate)` and `sample_upstream`, and specify `class TracingSettings(BaseModel):` in `tkati_core/settings.py` attached to `NodeSettings` as `tracing: TracingSettings = TracingSettings()`.

---

### 10. Align Headings with `AGENTS.md` (Preserving Session Subfolder), Add Implementation Phases, and Calibrate Memory Math [`egm-zoom-out`, `egm-structural`, `egm-style`, `egm-codebase-details`] - Severity: Major
* **Affected Scope:** [Lines 1-75](README.md#L1-L75), [Line 145](README.md#L145), [Lines 200-250](README.md#L200-L250), [Lines 254-381](README.md#L254-L381)
* **Discrepancy Description:**
  1. Reviewers noted `AGENTS.md` top-level headings (`## Context`, `## Goal` with `Done when:` / `Non-goals:`, `## Approach`, `## Design`, `## Implementation Steps`) and single-file vs. subfolder path. (Note: the subfolder `design-docs/2026-11-10-e2e-tracing/` was explicitly requested by the author for this EGM session and is preserved.)
  2. `implementation-readiness-standard.md` requires partitioning implementation steps into buildable, testable phases.
  3. Lines 145 and 244 omit the 32-bit Arrow offset buffer (`(N + 1) * 4` bytes = 4,004 bytes for 1,000 rows) when explaining the ~4.2 KiB figure, and line 249 calls `tracing.enabled = False` "zero-overhead passthrough mode".
  4. Lines 206–236 repeat `Rejected by author:`.
* **Rationale:** We can satisfy both `AGENTS.md` and `egm-review` rubrics by keeping the file in `design-docs/2026-11-10-e2e-tracing/README.md`, adopting clean section headers, grouping the file changes into 3 phases with verification checkpoints, calibrating the memory calculation, and removing `Rejected by author:`.
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Partitioning `Detailed Implementation Plan` into Phase 1 (Rust/PyO3 Header Bindings), Phase 2 (Python Kafka & ClickHouse Column/Header Handling), and Phase 3 (`_NodeBase` Span Lifecycle & Tests), adding the 4-byte-per-row offset buffer to the memory math, clarifying disabled no-op mode, and removing `Rejected by author:` resolves all structural and style findings.
* ***Proposed Edit:***
  * Location: Full document structure, memory footprint lines, `Alternatives Considered`, and `Detailed Implementation Plan`
  * Change: Include the 32-bit offset buffer (`4 * (N + 1)` bytes) in the ~4.2 KiB memory explanation, clarify that `tracing.enabled = False` is a no-op mode (while existing upstream headers are still passed through if present), remove `Rejected by author:` prefixes, and organize the target file modifications into 3 buildable/testable phases.

---

### 11. Clean Up Boilerplate Bullets, Hedged Types, Duplicate Status Header, and Footer Nits [`egm-style`, `egm-structural`, `egm-comprehension`] - Severity: Nit
* **Affected Scope:** [Lines 7-11](README.md#L7-L11), [Line 37](README.md#L37), [Line 55](README.md#L55), [Line 129](README.md#L129), [Line 142](README.md#L142), [Line 224](README.md#L224), [Lines 258-358](README.md#L258-L358), [Line 381](README.md#L381)
* **Discrepancy Description:** Duplicate `* **Status:** Draft` bullet under H1, 80-character hyphen dividers, undefined `tkati-sink-ch` in CUJ 1, undefined `global_offset` in line 142, redundant `* **Type / Scope**:` bullets on every file entry, hedged phrasing, and `*Drafted with EGM*` footer.
* **Rationale:** Improves readability and eliminates minor inconsistencies.
* ***AI Suggestion:*** PROPOSE FIX
* ***Justification:*** Straightforward textual cleanup across the document.
* ***Proposed Edit:***
  * Location: Lines 7–381
  * Change: Remove duplicate `* **Status:** Draft` bullet and `*Drafted with EGM*` footer, replace 80-character dividers with `---`, replace `tkati-sink-ch` with a node configured with `ClickhouseProducer`, replace `global_offset` with `self._rows_seen`, and drop redundant `Type / Scope` bullets.
