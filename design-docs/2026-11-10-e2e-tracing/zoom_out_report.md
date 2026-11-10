# Goldfish Review Report — Stage 5: Initial Zoom Out Review

**Target:** `design-docs/2026-11-10-e2e-tracing/README.md` (`1..381`)

## Reviewer Status

| Reviewer | Status |
| --- | --- |
| `egm-zoom-out` | completed |
| `egm-inventive-catalyst` | completed |

## Prioritized Findings (Blockers -> Major -> Nits)

### 1. Broken W3C Trace Propagation & Split-Brain Responsibilities Across Layers - Severity: Blocker
* **Reviewer Lens:** `egm-zoom-out`, `egm-inventive-catalyst`
* **Affected Scope:** `## Technical Plan (Architecture)` (Lines 120–185) and `## Detailed Implementation Plan` (Lines 301–338)
* **Discrepancy Description**:
  1. **Outbound header never updates to the child span's context**: `KafkaConsumer.read_arrow()` attaches the incoming `traceparent` header as `tkati_traceparent`. In `_NodeBase.consume_arrow()`, the node starts a child span (`tracer.start_span(...)`), which gets a new `span_id`, and stores it in `dict[int, Span]`. However, `tkati_traceparent` in the Arrow table is never rewritten with the child span's `span_id`. When `KafkaProducer.produce_arrow()` extracts `tkati_traceparent` from the table, it forwards the upstream parent's `traceparent` unchanged. Downstream nodes attach their spans as siblings under the root span rather than forming the causal parent-child chain required by CUJ 1.
  2. **Coupled Drop-Observation & Broken Span Lookup**: `_NodeBase` stores active spans in `dict[int, Span]` keyed by initial row index, while `_dedupe_batch()` in `tkati-node-dedup` is tasked with inverting `keep_mask`, looking up spans by `tkati_traceparent` string, emitting `dedup_dropped` events, and closing spans. `_dedupe_batch(batch, field_name, store, pending)` is a pure tabular helper with no reference to `node` or its span registry, and hardcoding drop detection in `tkati-node-dedup` leaves row filtering in `tkati-node-el` uninstrumented.
  3. **Read-Ahead Thread Separation of Concerns**: Placing head stride sampling inside `KafkaConsumer.read_arrow()` forces `KafkaConsumer` to depend on `TracingSettings` and execute sampling on the background `tkati-read-ahead` thread before a batch is taken by the loop thread.
* **Rationale & Recommendation**:
  Centralize trace lifecycle, child context rewriting, and drop detection in `_NodeBase` (Extraction / Self-Service Pattern):
  - When `_NodeBase` starts a child span for each non-null row in `tkati_traceparent`, format the child span's W3C `traceparent` string and replace the column values in `event.data` so surviving rows carry the child `span_id` into `output_arrow`.
  - In `_NodeBase._take_batch(event, output_arrow, ...)`, compare the set of sampled `tkati_traceparent` strings present on `event.data` against those surviving in `output_arrow`. Any sampled traceparent absent from `output_arrow` was filtered/dropped by the node; `_NodeBase` can record the drop event and close those spans centrally without modifying `tkati-node-dedup/src/tkati_node_dedup/main.py`.

### 2. Stripping `tkati_traceparent` Too Early in `ClickhouseProducer` Breaks DLQ Trace Continuity - Severity: Major
* **Reviewer Lens:** `egm-zoom-out`, `egm-inventive-catalyst`
* **Affected Scope:** `#### 3. Outbound Serialization Boundaries` (Lines 158–160) & `#### 9. packages/tkati-core/tkati_core/clickhouse/producer.py` (Lines 317–323)
* **Discrepancy Description**:
  Step 9 specifies dropping `"tkati_traceparent"` in both `ClickhouseProducer.produce_arrow()` and `_insert_with_retry()`, while claiming the producer will record error attributes if routed to the DLQ. If `produce_arrow()` strips `"tkati_traceparent"` before calling `_insert_with_dlq_fallback()`, then during recursive chunk splitting (`_table_slices`), the isolated bad row passed to `dlq_producer.produce_arrow(table)` has already lost its `"tkati_traceparent"` column and cannot propagate its trace header to the DLQ Kafka topic. Furthermore, `ClickhouseProducer` is called synchronously inside `node.done()` and should not duplicate span management already owned by `_NodeBase`.
* **Rationale & Recommendation**:
  Keep `"tkati_traceparent"` attached throughout `ClickhouseProducer.produce_arrow()` and `_insert_with_dlq_fallback()`, and strip it exclusively inside `_insert_with_retry()` right before `ch_client.insert_arrow()`. This preserves `tkati_traceparent` across `_table_slices()` so `dlq_producer.produce_arrow(table)` automatically forwards the trace header to the DLQ topic.

### 3. Document Structure & Naming Violates Repository Design Doc Convention (`AGENTS.md`) - Severity: Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** File path (`design-docs/2026-11-10-e2e-tracing/README.md`) and top-level headings (Lines 1–381)
* **Discrepancy Description**:
  Per `AGENTS.md`, required top-level section headings are `## Context`, `## Goal` (with `Done when:` and `Non-goals:` lists), `## Approach`, `## Design`, and `## Implementation Steps`. The document instead uses `## Overview`, `## The Problem`, `## Goals`, `## Non-Goals`, `## Technical Plan (Architecture)`, and `## Detailed Implementation Plan`.
* **Rationale & Recommendation**:
  Align the section headers and `Done when:` / `Non-goals:` structure with `AGENTS.md` conventions.

### 4. Unresolved Open Questions & Redundant Configuration (`YAGNI`) - Severity: Major
* **Reviewer Lens:** `egm-zoom-out`
* **Affected Scope:** `## Open Questions` (Lines 47–50), `#### 5. Configuration Model` (Lines 186–196), and `#### 6. settings.py` (Lines 290–300)
* **Discrepancy Description**:
  - Line 48 leaves an open question on OTel environment variables vs. `[tracing]` TOML config, while Lines 188–195 and 293–298 define `enabled`, `service_name`, `trace_each_nth`, and `sample_upstream` in `TracingSettings` alongside `OTEL_SERVICE_NAME` / `OTEL_EXPORTER_OTLP_ENDPOINT`.
  - `sample_upstream: bool = True` is speculative configuration surface area (YAGNI) given the goal that nodes strictly follow upstream tags.
  - Line 293 proposes a `dataclass` with `field(default_factory=TracingSettings)`, whereas `tkati_core/settings.py` uses `pydantic.BaseModel`.
* **Rationale & Recommendation**:
  Resolve Open Questions, remove speculative `sample_upstream` toggle, rely on standard `OTEL_*` environment variables for SDK/exporter config, and define `TracingSettings(BaseModel)` using Pydantic to match `tkati_core/settings.py`.

### 5. Altitude Drops & Premature Span Closure Trade-Off in `PipelinedNode` - Severity: Nit
* **Reviewer Lens:** `egm-zoom-out`, `egm-inventive-catalyst`
* **Affected Scope:** `## Open Questions` (Line 49), `## Technical Plan (Architecture)` (Lines 120–185), `### Alternative F` (Lines 232–236)
* **Discrepancy Description**:
  - The high-level architecture section drops into low-level code snippets and bit-level calculations that belong in Detailed Design / Implementation Steps.
  - In `PipelinedNode`, closing spans immediately in `done()` omits the `after_commit` phase (where `tkati-node-dedup` writes to RocksDB in `_mark_seen`) and exports `StatusCode.OK` before broker delivery acknowledgment. Attaching active batch spans to `_Finished` in `PipelinedNode` (recording a `processing_done` event at `done()` and calling `span.end()` in `_commit()`) would capture both CPU transform time and commit settlement without a separate span registry.
* **Rationale & Recommendation**:
  Keep the high-level architecture section focused on component boundaries and data flow, and either document the `after_commit` trade-off explicitly or close `PipelinedNode` spans in `_commit()`.
