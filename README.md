# tkati Blueprint

## System Overview
`tkati` is a high-throughput streaming dataflow framework designed for building, executing, and observing Kafka-to-Kafka and Kafka-to-ClickHouse stream processing pipelines. Built around native `librdkafka` C extensions with PyO3 bindings and Apache Arrow in-memory batch representations, `tkati` provides resilient worker node loops, deterministic batch commits, streaming deduplication, and topology visualization.

## Subdirectories & System Boundaries
- `packages`: [./packages/README.md](./packages/README.md) - Intermediate blueprint containing all workspace packages, libraries, and node implementations:
  - `packages/tkati-core`: [./packages/tkati-core/README.md](./packages/tkati-core/README.md) - Foundational node loop harnesses (`SyncNode`, `PipelinedNode`), native Kafka consumer/producer wrappers, ClickHouse batch writers, settings models, and `LoopStats` phase telemetry.
  - `packages/tkati-dashboard`: [./packages/tkati-dashboard/README.md](./packages/tkati-dashboard/README.md) - Local FastAPI web dashboard and topology graph viewer for inspecting pipeline nodes, topic schemas, and consumer lag.
  - `packages/tkati-node-dedup`: [./packages/tkati-node-dedup/README.md](./packages/tkati-node-dedup/README.md) - Streaming deduplication node utilizing an embedded, processing-time bucketed RocksDB store with fail-forward duplicate delivery guarantees.
  - `packages/tkati-node-el`: [./packages/tkati-node-el/README.md](./packages/tkati-node-el/README.md) - Generic extract-load pipeline node supporting dynamic user-defined Polars transformations and recursive bisection dead-letter queue (DLQ) isolation.

## Global Invariants
- **Unified Workspace Versioning**: All workspace packages, the root `pyproject.toml`, inter-package dependencies (`tkati-core==0.11.0`), and `packages/tkati-core/Cargo.toml` must share an identical version number.
- **Type Checking Standard**: All Python code across packages must pass type checking with Astral's `ty` (`uv run ty check packages/`).
- **Explicit Batch Durability**: Automatic Kafka offset commits (`enable.auto.commit`) are strictly disabled; node loops commit offsets only after downstream durability (Kafka ack or ClickHouse insert) is confirmed.
- **Fail-Forward Dedup Semantics**: In duplicate detection, internal store errors default to treating keys as unseen to prevent event loss.
- **At-Least-Once Delivery**: Node failures or unhandled exceptions rewind input offsets to the last committed checkpoint, triggering reprocessing upon restart.

## Global Side Effects
- Consumes and produces streaming messages on Apache Kafka clusters over native TCP sockets.
- Inserts columnar data blocks into ClickHouse databases via HTTP/native client protocols.
- Binds Prometheus HTTP metrics endpoints (default `:8000`) for loop phase metrics and performance telemetry.
- Creates and manages local RocksDB files on disk for streaming deduplication state.

## System Verification
- Run: `uv run pytest`
- Run: `uv run ty check packages/`
- Run: `cargo test --manifest-path packages/tkati-core/Cargo.toml`
