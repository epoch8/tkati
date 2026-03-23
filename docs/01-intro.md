# Tkati — Overview

## Purpose

This system provides a declarative way to define, manage, and operate data pipelines as directed graphs. Pipelines are described in **Jsonnet**, compiled to a concrete manifest, and executed in a runtime that coordinates Kafka topics and Python processing nodes.

## Core Concepts

### Schemas

Schemas define the shape of data flowing through the system. They serve as contracts between producers and consumers and are the basis for serialization (Arrow IPC / Parquet), schema evolution, and documentation.

Each schema is defined once and referenced by name from topics and nodes.

### Data Streams (Kafka Topics)

Topics are the edges of the dataflow graph. Each topic is typed — it carries records of a specific data structure — and has configuration for partitioning, retention, and compaction appropriate to its role (event log, state changelog, sink, etc.).

Topics are declared explicitly so the system can provision, validate, and version them independently of the nodes that use them.

### Processing Nodes

Nodes are the vertices of the graph. A node:

- **consumes** one or more input topics (or none, for generator nodes that self-produce on a timer)
- optionally **produces** one or more output topics
- encapsulates a Python function that transforms batches of Arrow record batches
- runs as its own **Kubernetes Deployment** — each node is an independent, separately scalable process

Nodes are stateless by default. State (e.g. aggregations, joins) is managed via changelog topics or external stores declared in the graph.

## Execution Model

The system is **small-batch oriented**:

- Each node polls its input topics and accumulates records into micro-batches.
- Generator nodes (`ZeroToOne`) have no input topics — the runner calls their `generate()` method on a configurable tick interval and produces the result directly.
- Batches are materialized as **Apache Arrow** record batches in memory.
- User logic is plain Python operating on Arrow tables — zero-copy, vectorized, interoperable with pandas/polars/DuckDB.
- Output record batches are serialized and produced to output topics.

This model trades pure streaming latency for operational simplicity, predictable resource usage, and the ability to leverage Arrow's columnar compute kernels natively.

## Configuration via Jsonnet

The entire graph — schemas, topics, nodes, and their wiring — is described in Jsonnet. Jsonnet's templating and import system allows:

- reusable schema and topic templates
- environment-specific overrides (dev / staging / prod)
- composition of sub-graphs into larger pipelines

The Jsonnet definition compiles to a plain JSON manifest that the runtime and provisioning tools consume.

## System Boundaries

| Layer | Responsibility |
|---|---|
| Jsonnet definitions | Declare the graph: schemas, topics, nodes, wiring |
| Manifest compiler | Validate and render Jsonnet to a deployable JSON manifest |
| Provisioner | Create / update Kafka topics and one Kubernetes Deployment per node |
| Runtime | Execute nodes: poll → batch → process (Arrow/Python) → produce |
| Schema registry | Store and enforce data structure versions |

## Design Goals

- **Declarative** — the graph definition is the source of truth; the runtime derives everything from it.
- **Typed** — schemas are explicit; mismatches are caught at compile/deploy time, not at runtime.
- **Simple runtime** — nodes are ordinary Python functions; no framework-specific decorators or magic.
- **Composable** — sub-graphs can be defined as Jsonnet libraries and assembled into larger systems.
- **Small footprint** — optimized for batch sizes of seconds to minutes, not microsecond latency.
