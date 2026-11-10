# Design Document Drift Report (2026-11-10)

## 2026-09-26-explicit-batch-commit.md

No drift detected.

## 2026-09-26-node-el-loop-stats.md

No drift detected.

## 2026-09-26-pipelined-worker-loop.md

No drift detected.

## 2026-09-26-worker-loop-harness.md

No drift detected.

## 2026-09-27-perf-report-lines.md

No drift detected.

## 2026-09-27-tkati-core-audit.md

No drift detected.

## 2026-09-28-clickhouse-error-classification.md

No drift detected.

## 2026-10-01-kafka-config-passthrough.md

No drift detected.

## 2026-10-05-node-el-transform.md

No drift detected.

## 2026-10-28-clickhouse-insert-deduplication.md

No drift detected.

## 2026-11-10-e2e-tracing/README.md

* `_init_tracer` in `packages/tkati-core/tkati_core/node.py` dynamically loads `opentelemetry.exporter.otlp.proto.grpc.trace_exporter` via `importlib.import_module` rather than static import to prevent `ty` static analysis failures when optional `[project.optional-dependencies]` tracing package is not installed.
* `_init_tracer` automatically sets `insecure=True` on `OTLPSpanExporter` when connecting to `localhost` or `127.0.0.1` endpoints without requiring explicit `OTEL_EXPORTER_OTLP_INSECURE=true`.
* `_NodeBase.__exit__` in `packages/tkati-core/tkati_core/node.py` calls `provider.force_flush()` before process exit to guarantee OTLP background batch delivery in short-lived processes.
* `memory_node` and `memory_pipelined_node` in `packages/tkati-core/tkati_core/testing.py` accept an optional `tracing: TracingSettings | None = None` parameter to enable broker-free pipeline testing with span tracking.
* The design document is missing the post-ship `## Implementation notes (as built)` section capturing the above runtime adjustments.
