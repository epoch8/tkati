# Goldfish Review Report — Stage 7: Critic Review

**Target:** `design-docs/2026-11-10-e2e-tracing/README.md` (`1..253`)

## Reviewer Status

| Reviewer | Status |
| --- | --- |
| `egm-design` | completed |
| `egm-structural` | completed |
| `egm-style` | completed |

## Prioritized Findings (Blockers -> Major -> Nits)

### 1. Missing Mandatory `Risks & Mitigations` / `Risks & Limits` Section - Severity: Blocker
* **Reviewer Lens:** `egm-design`, `egm-structural`
* **Affected Scope:** `## Non-Functional Requirements` (Lines 240–253)
* **Discrepancy Description**:
  `feature-spec-standard.md` mandates an explicit **Risks & Limits / Risks & Mitigations** section identifying failure modes, security implications, scalability limits, and their mitigations. The document provides only `Non-Functional Requirements` (lines 240–250) and omits analysis of:
  1. Upstream sampling floods / DoS (an upstream producer setting `flags=01` on 100% of records).
  2. Malformed or oversized W3C `traceparent` headers from untrusted Kafka producers.
  3. Sensitive data / PII leakage in span attributes (e.g., attaching raw `{"dedup.key": key}` in CUJ 3, line 66).
  4. OTLP collector unavailability and `BatchSpanProcessor` queue saturation/drop behavior.
* **Rationale & Recommendation**:
  Add a dedicated `## Risks & Mitigations` section before line 253 addressing these failure modes, security/privacy boundaries, and concrete mitigations (such as a per-batch `max_spans_per_batch` cap and attribute sanitization).

### 2. Unbounded Upstream Sampling & Unsampled Header Extraction (`flags=00`) Can Stall Worker Loop - Severity: Blocker
* **Reviewer Lens:** `egm-design`
* **Affected Scope:** Lines 120–169, 186–195
* **Discrepancy Description**:
  1. **Unsampled W3C headers (`flags=00`) treated as sampled**: In Component 1 (line 123), `NativeConsumer::poll_step` extracts every `"traceparent"` header into `RawBatch`. In Component 4 (lines 165–167), `consume_arrow()` starts a span for every non-null entry in `tkati_traceparent`. Upstream gateways often inject `traceparent` on 100% of messages with `flags=00` (not sampled). Extracting unsampled headers into `tkati_traceparent` without filtering by `flags & 0x01 == 1` will cause 100% of rows to be non-null and trigger per-row Python span creation across the entire batch.
  2. **No per-batch cap when `sample_upstream = true`**: A misconfigured upstream service sending `-01` on all messages will bypass `trace_each_nth` and force `tkati` to create thousands of Python spans per batch, violating the 3% throughput gate.
* **Rationale & Recommendation**:
  Ignore incoming `traceparent` headers whose sampled bit is not set (`flags & 0x01 == 0`) so those rows remain `null` in `tkati_traceparent`, and enforce a configurable per-batch cap (`max_spans_per_batch: int = 16`).

### 3. Broken Parent-Child Trace Hierarchy Across Multi-Node Hops and Head Sampling - Severity: Blocker
* **Reviewer Lens:** `egm-design`
* **Affected Scope:** Lines 53–62, 139–172
* **Discrepancy Description**:
  1. **Orphaned root `span_id` on head-sampled messages**: In Component 2 (lines 140–143), `KafkaConsumer.read_arrow()` fabricates a `traceparent` string `00-{32_hex_trace_id}-{16_hex_span_id}-01` with a generated `16_hex_span_id` without exporting a span for that ID. Then `consume_arrow()` (lines 166–167) parses that string as `parent_context` and creates a child span, leaving a broken trace with a missing root span in Jaeger/Tempo.
  2. **`tkati_traceparent` is never updated with the current node's child `span_id` before outbound production**: `consume_arrow()` starts a child span for each sampled row, but never writes the child span's new W3C `traceparent` (`00-{trace_id}-{child_span_id}-01`) back into `tkati_traceparent`. Consequently, `KafkaProducer.produce_arrow()` forwards the original incoming `traceparent` unchanged, flattening the trace into sibling spans instead of a parent-child chain.
* **Rationale & Recommendation**:
  When `consume_arrow()` starts a `node_process` span for a sampled row, format that new span's context as a W3C `traceparent` and replace the row's entry in `tkati_traceparent` so downstream hops parent to the current node's span. For head-sampled untraced messages, start a root span (no parent context) in the ingest node rather than fabricating an unexported parent `span_id`.

### 4. Span Leaks on Batch Failure/Shutdown and Contradictory Sink/Dedup Span Ownership - Severity: Major
* **Reviewer Lens:** `egm-design`
* **Affected Scope:** Lines 158–185
* **Discrepancy Description**:
  1. **Unclosed spans on exception or early loop exit**: Spans are started when `consume_arrow()` yields a `Batch` and ended only when `node.done()` completes. In `_NodeBase.__exit__`, if user transform code or producer delivery raises an exception (triggering batch rewind), `node.done()` never completes and active spans are leaked without recording `StatusCode.ERROR` or calling `span.end()`.
  2. **Contradictory `ClickhouseProducer` span ownership & DLQ stripping bug**: Line 159 strips `tkati_traceparent` in `_insert_with_retry`, whereas the diagram (line 98) strips it in `ClickhouseProducer.produce_arrow()`. Stripping in `produce_arrow()` before `_insert_with_dlq_fallback` strips `tkati_traceparent` from rows routed to `dlq_producer.produce_arrow(table)`. Also, `ClickhouseProducer` has no reference to `node.py`'s `dict[int, Span]`.
  3. **Broken span lookup in `tkati-node-dedup` and silent drops in `tkati-node-el`**: `_dedupe_batch()` has no reference to `node` or its `dict[int, Span]` (which is indexed by row `int`, not `traceparent` string as assumed in lines 178–183), and `table.filter(dropped_mask).column("tkati_traceparent").drop_null()` loses the row index and dedup key needed for the `{"dedup.key": key, "dedup.bucket": bucket_id}` attributes promised in CUJ 3 (line 66).
* **Rationale & Recommendation**:
  Centralize span lifecycle in `_NodeBase`: close active spans in `done()` (or via an explicit `node.drop_span(...)` helper / input-vs-output diff), abort/end any uncommitted spans with `StatusCode.ERROR` in `_NodeBase.__exit__` on failure, and strip `tkati_traceparent` in `ClickhouseProducer` only inside `_insert_with_retry()`.

### 5. `KafkaProducer` `"arrow-batch"` Format Not Addressed When Extracting Headers - Severity: Major
* **Reviewer Lens:** `egm-design`
* **Affected Scope:** Lines 148–157
* **Discrepancy Description**:
  `KafkaProducer.produce_arrow()` supports both `"json"` (one Kafka message per row) and `"arrow-batch"` (the entire `pa.Table` serialized into a single Arrow IPC message, where `len(messages) == 1`). Unconditionally extracting `traceparents` (length $N$ rows), dropping the column, and passing $N$ headers to `NativeProducer.enqueue()` causes a length mismatch (`1 != N`) in `"arrow-batch"` mode and strips the column from the Arrow IPC stream.
* **Rationale & Recommendation**:
  Explicitly specify that `KafkaProducer.produce_arrow()` preserves `tkati_traceparent` inside the Arrow IPC stream payload for `self.format == "arrow-batch"`, and only extracts/strips `tkati_traceparent` into per-message Kafka headers when `self.format == "json"`.

### 6. Uncalibrated Memory & Passthrough Overhead Claims (`CALIBRATE`) - Severity: Major
* **Reviewer Lens:** `egm-style`
* **Affected Scope:** Lines 145, 244, 249
* **Discrepancy Description**:
  1. Line 145 states that null values in an Arrow `StringArray` "occupy 1 bit in the validity bitmap and 0 bytes in the string data buffer" and concludes that a 1,000-row batch with 2 sampled rows uses "~4.2 KiB" (repeated in line 244). This omits the 32-bit offset buffer (`(N + 1) * 4` bytes = 4,004 bytes for 1,000 rows), which accounts for ~95% of the 4.2 KiB overhead.
  2. Line 249 claims that when `[tracing]` is disabled, `tkati` operates in "zero-overhead passthrough mode". Passthrough (extracting headers into `tkati_traceparent` and re-emitting on produce) still incurs Arrow column and list allocation overhead, whereas bypassing header extraction is a no-op mode, not passthrough.
* **Rationale & Recommendation**:
  Explicitly include the 4-byte-per-row offset buffer (`4 * (N + 1)` bytes) in the memory math on lines 145 and 244, and specify whether `tracing.enabled = False` skips wire header extraction entirely (no-op mode) or forwards headers without creating spans, dropping the "zero-overhead" buzzword.

### 7. Unlinked Author Attribution in `Alternatives Considered` & Repository Heading Conventions - Severity: Major
* **Reviewer Lens:** `egm-structural`, `egm-style`
* **Affected Scope:** Lines 1–75, 200–237
* **Discrepancy Description**:
  1. All six alternatives (`Alternative A` through `Alternative F`) prefix their rejection rationale with `* **Reason for Rejection**: Rejected by author: ...`.
  2. `AGENTS.md` specifies top-level headings `## Context`, `## Goal` (`Done when:` / `Non-goals:`), `## Approach`, `## Design`, and `## Implementation Steps`.
* **Rationale & Recommendation**:
  Remove the redundant `Rejected by author:` prefix from lines 206, 212, 218, 224, 230, and 236, and align section headings with `AGENTS.md` conventions.

### 8. Style, Terminology, and Formatting Nits - Severity: Nit
* **Reviewer Lens:** `egm-design`, `egm-structural`, `egm-style`
* **Affected Scope:** Lines 1–10, 11–252
* **Discrepancy Description**:
  - Lines 48–49 list Open Questions that contradict settled sections later in the document (Lines 186–195, 232–236).
  - Line 142 uses undefined `global_offset` in `(global_offset + row_index) % stride == 0` instead of a consumer-local monotonic row counter.
  - Line 129 claims untagged messages incur "zero allocation" while `Vec<Option<String>>` allocates container elements; narrow to "no `OwnedHeaders` allocation in `librdkafka`."
  - Lines 37, 172, 236 use abstract phrasing ("execution truth" / "truth of execution"); replace with concrete terms ("synchronous batch processing duration").
  - Line 224 uses "leverages Arrow's vectorized SIMD operations (`filter`, `take`)"; simplify to "uses Arrow's built-in `filter` and `take` operations."
  - Lines 7–9 duplicate `status: DRAFT` from YAML frontmatter and use 80-character hyphen dividers (`--------...`).
* **Rationale & Recommendation**:
  Resolve settled Open Questions, clarify the monotonic row counter, tighten imprecise phrasing, and clean up duplicate metadata headers.
