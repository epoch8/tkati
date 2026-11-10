**What this proposes**
This document proposes adding end-to-end OpenTelemetry distributed tracing across `tkati` streaming pipelines by extracting W3C `traceparent` headers from incoming Kafka messages in the Rust `NativeConsumer` and attaching them as a nullable `tkati_traceparent` column on in-memory PyArrow tables. Ingest nodes can also apply stride-based head sampling (`trace_each_nth`) to generate trace contexts for untraced messages, while intermediate processing nodes start child spans when yielding a batch (`consume_arrow()`) and close them when batch processing completes (`node.done()`). Before data leaves a node, `KafkaProducer` strips the `tkati_traceparent` column and re-attaches the values as outbound Kafka record headers, while `ClickhouseProducer` strips the column before database insertion and records a concluding insert span. Finally, deduplication nodes inspect rows filtered out during batch deduplication and emit a `dedup_dropped` event on the corresponding spans before closing them with `StatusCode.OK`.

**How the existing system works, per this document**
`tkati` is a high-throughput streaming framework where nodes (`SyncNode` and `PipelinedNode` in `tkati-core`, plus processing nodes like `tkati-node-el` and `tkati-node-dedup`) consume, transform, and produce batches of data as PyArrow tables (or lists of dicts via `consume_pylist`). Under the hood, a Rust PyO3 extension (`NativeConsumer` and `NativeProducer` wrapping `librdkafka`) polls raw message batches (`RawBatch`) from Kafka—currently discarding Kafka headers—and deserializes newline-delimited JSON into PyArrow tables via `parse_ndjson`. After business logic transforms or filters the batch (such as `_dedupe_batch()` checking `BucketedDedupStore` in `tkati-node-dedup`), `KafkaProducer.produce_arrow()` serializes the Arrow table back to JSON and enqueues an `EncodedBatch` to `NativeProducer`, or `ClickhouseProducer` writes the Arrow table directly to ClickHouse via `insert_arrow()`. Currently, observability is limited to aggregate `LoopStats` Prometheus counters and phase timers, with no per-event trace correlation across nodes.

---

### Comprehension Findings

#### 1. Child Span Context Propagation (Parent Span ID Updating) Across Pipeline Hops
* **Severity**: Major
* **Affected Scope**: `## Critical User Journeys (CUJs)` (lines 53–56), `### Component Details` -> `3. Outbound Serialization Boundaries` (lines 148–157) & `4. Node Lifecycle & Span Management` (lines 162–173)
* **Discrepancy Description & Rationale**: When a processing node starts a child span (`tracer.start_span(name=f"{node_name} process", context=parent_context)`) for a sampled row, that child span gets a new OpenTelemetry `span_id`. However, the document states that `table.column("tkati_traceparent")` is simply preserved through user transforms and extracted directly by `KafkaProducer.produce_arrow()` to be forwarded to the next Kafka topic. It never explains if or where the `tkati_traceparent` column value is updated with the current node's new `span_id` before producing to Kafka, or whether all nodes become flat siblings under the original upstream parent span. Furthermore, CUJ 1 (line 56) mentions four child spans (`raw-events receive`, `tkati-node-el process`, `tkati-node-dedup process`, and `ClickHouse insert`), without explaining what component creates `raw-events receive`.

#### 2. Undefined Mechanism for `tkati-node-dedup` to Access Active Spans and Emit Specified Attributes
* **Severity**: Major
* **Affected Scope**: `## Critical User Journeys (CUJs)` -> `CUJ 3` (lines 63–66), `### Component Details` -> `4. Node Lifecycle & Span Management` (lines 168–184), and `## Detailed Implementation Plan` -> `11. packages/tkati-node-dedup/src/tkati_node_dedup/main.py` (lines 332–337)
* **Discrepancy Description & Rationale**: There are three gaps in how `tkati-node-dedup` handles dropped spans:
  1. **Span Lookup**: Line 168 states that active spans are stored in `tkati_core/node.py` in a batch tracker `dict[int, Span]` indexed by integer `row_index`. However, the snippet in lines 178–180 filters the table (`dropped_traces = table.filter(dropped_mask).column("tkati_traceparent").drop_null()`), losing the original integer row indices, and iterates over string values `for tp in dropped_traces:` with a comment `# Find corresponding span`. It is unclear how `_dedupe_batch()` in `tkati-node-dedup` accesses the span registry inside `node.py` or maps a `traceparent` string (which may not even be unique if the duplicate records carried the exact same upstream `traceparent`) back to the `dict[int, Span]` indexed by row index.
  2. **Attribute Mismatch**: CUJ 3 (line 66) states that `dedup_dropped` is emitted with attributes `{"dedup.key": key, "dedup.bucket": bucket_id}`, whereas the architecture code snippet (line 181) emits `{"dedup.reason": "seen_key"}`.
  3. **Filtering in `tkati-node-el`**: `Overview` (line 17), `The Problem` (line 26), and the Mermaid diagram (`Dedup / Filter Drop Handling`, line 87) state that filtering nodes / `tkati-node-el` also drop records and emit status events, but neither the Technical Plan nor the Detailed Implementation Plan includes any changes to `tkati-node-el` or explains how general row filtering in `tkati-node-el` closes spans for filtered-out rows.

#### 3. Contradiction Between `Open Questions` and `Technical Plan` / `Detailed Implementation Plan`
* **Severity**: Major
* **Affected Scope**: `## Open Questions` (lines 46–50) vs. `### Component Details` -> `5. Configuration Model` (lines 186–195), `### Alternative F` (lines 232–236), and `## Detailed Implementation Plan` -> `6. settings.py` (lines 290–299)
* **Discrepancy Description & Rationale**:
  * **Question 1** (line 48) asks whether tracer configuration should be supplied strictly via OTel environment variables or mirrored within `AppSettings` under a `[tracing]` TOML block (and names the class `AppSettings`), while Section 5 (line 187) and Step 6 (line 298) definitively specify adding `TracingSettings` under `[tracing]` in `NodeSettings` alongside OTel environment variables.
  * **Question 2** (line 49) asks how `PipelinedNode` asynchronous librdkafka delivery failures after `node.done()` should be reported, while `Alternative F` (lines 232–236) already rejects deferred delivery settlement for `PipelinedNode` spans and states that "outbound delivery is tracked independently" (though how outbound delivery is tracked independently is never explained).

#### 4. Ambiguous API and Data Types Between Rust (`RawBatch`, `NativeProducer`) and Python
* **Severity**: Major
* **Affected Scope**: `### Component Details` -> `1. PyO3 Native Extension` (lines 120–129) & `2. Vectorized In-Memory Representation` (lines 134–136) vs. `## Detailed Implementation Plan` -> `3, 4, 5, 7` (lines 268–307)
* **Discrepancy Description & Rationale**: A fresh reader cannot tell what the exact method name and return type on `RawBatch` or signature on `NativeProducer.enqueue` are because the sections contradict each other:
  * **Consumer side**: Line 125 says `RawBatch` exposes `traceparents` returning a `pyo3_arrow` or Python list representation; line 135 calls `raw_batch.traceparents_arrow()` returning a PyArrow `StringArray` directly; line 286 (`_native.pyi`) declares `def traceparents(self) -> list[str | None]: ...` and line 304 (`consumer.py`) calls `raw_batch.traceparents()` and constructs the `pa.StringArray` in Python.
  * **Producer side**: Line 127 says `EncodedBatch` is extended/supplemented to carry `Sequence[tuple[bytes, str | None, str | None]]` (payload, key, traceparent header), whereas lines 280 and 287 leave `EncodedBatch` unchanged and instead add a separate `headers: Sequence[str | None] | None = None` parameter to `NativeProducer.enqueue(self, batch: EncodedBatch, tag: int = 0, headers: ...)`.

#### 5. Undefined Behavior for `sample_upstream` and W3C `flags` Handling
* **Severity**: Major
* **Affected Scope**: `## Goals` (lines 30, 33), `### Component Details` -> `2. Vectorized In-Memory Representation` (lines 139–144) & `4. Node Lifecycle & Span Management` (lines 163–168), and `5. Configuration Model` (line 193)
* **Discrepancy Description & Rationale**:
  * Line 33 states that ingest nodes honor upstream sampled flags (`flags & 0x01 == 1`) when headers are already present, and line 193 introduces `sample_upstream = true # Propagate upstream sampled flags (default: true)`. However, the Technical Plan never specifies what happens when an upstream message arrives with an unsampled W3C header (`flags == 00`), or when `sample_upstream = false`: is its `tkati_traceparent` entry set to `None` in the Arrow column so downstream nodes don't create spans for it, or is the header preserved in `tkati_traceparent` while `consume_arrow()` inspects the `01` flag before starting a span?
  * Line 30 mentions propagating `(and optional tracestate)` headers across Kafka topics, but `tracestate` is never mentioned again in the Technical Plan or Detailed Implementation Plan.

#### 6. Unexplained `ClickhouseProducer` Span Access and `tkati-sink-ch` Component Reference
* **Severity**: Nit
* **Affected Scope**: `## Critical User Journeys (CUJs)` -> `CUJ 1` (line 55), `### Component Details` -> `3. Outbound Serialization Boundaries` (lines 158–160), and `## Detailed Implementation Plan` -> `9. packages/tkati-core/tkati_core/clickhouse/producer.py` (lines 317–322)
* **Discrepancy Description & Rationale**:
  * Line 55 refers to `tkati-sink-ch`, which is not defined anywhere else in the document (the implementation plan only modifies `packages/tkati-core/tkati_core/clickhouse/producer.py`).
  * Line 160 says that after ClickHouse acknowledges the block, "the corresponding row spans are marked `StatusCode.OK` and closed", whereas line 321 says `ClickhouseProducer` records "a database insert child span (`f"{table_name} insert"`) and close[s] it with `StatusCode.OK` upon server acknowledgment". It is unclear whether `ClickhouseProducer` creates one child span per row or one span per batch insert, and how `ClickhouseProducer.produce_arrow()` obtains the parent trace contexts if it strips `"tkati_traceparent"` before inserting.

#### 7. Undefined Terms in Head Stride Sampling (`global_offset`) and `consume_pylist` Passthrough
* **Severity**: Nit
* **Affected Scope**: `## Non-Goals` (line 43) and `### Component Details` -> `2. Vectorized In-Memory Representation` (lines 140–143)
* **Discrepancy Description & Rationale**:
  * Line 142 uses `(global_offset + row_index) % stride == 0` to trigger head sampling, without defining what `global_offset` is (e.g., whether it is a consumer-local monotonic message counter across polls or a Kafka partition offset).
  * Line 43 states that `consume_pylist` "will support basic header passthrough only, without per-row span lifecycle management," but the Technical Plan and Detailed Implementation Plan never mention how `consume_pylist` passes headers through to the producer if it doesn't use the `tkati_traceparent` Arrow column.
