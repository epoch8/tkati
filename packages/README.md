# Packages

## Overview
The `packages` directory contains the core library abstractions, streaming pipeline worker nodes, and visual telemetry tooling for the `tkati` streaming dataflow ecosystem. Each package operates as an independent, typed Python workspace member sharing a unified workspace version.

## Subdirectories & Boundaries
- `tkati-core`: [./tkati-core/README.md](./tkati-core/README.md) - Core streaming node execution harnesses (`SyncNode`, `PipelinedNode`), native Kafka consumer/producer bindings, ClickHouse writers, and loop telemetry.
- `tkati-dashboard`: [./tkati-dashboard/README.md](./tkati-dashboard/README.md) - Local FastAPI web dashboard and topology graph viewer for serialized dataflow fragments.
- `tkati-node-dedup`: [./tkati-node-dedup/README.md](./tkati-node-dedup/README.md) - Streaming deduplication node using an embedded, hour-bucketed RocksDB store with at-least-once delivery guarantees.
- `tkati-node-el`: [./tkati-node-el/README.md](./tkati-node-el/README.md) - Generic extract-load pipeline node with dynamic Polars transformation and recursive bisection DLQ routing.
