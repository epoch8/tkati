# Dataflow serialization

## Overview

Today we compose dataflows from tkati nodes, but we do not have a durable way to persist the dataflow shape for later reuse by other tools, such as a visualization dashboard or a configuration browser.

This document proposes an extendable serialization format for a tkati dataflow so that a graph can be stored, inspected, validated, and rendered without depending on the live runtime process.

The core idea is simple: a dataflow is a graph, and the graph is serialized as a directory of fragments, where each fragment is a JSON document describing part of the dataflow.

## Goals

- Preserve the structure of a running tkati dataflow as a plain, portable artifact.
- Support both source/sink nodes and processing nodes in one graph representation.
- Allow a visualization tool to reconstruct node relationships from serialized metadata.
- Keep the format easy to evolve as the system adds more node types and runtime behavior.
- Favor explicit, human-readable JSON over hidden runtime state.

## Non-goals

- Serializing runtime metrics, offsets, or ephemeral execution state.
- Capturing every operational detail of a running process.
- Defining how to execute a serialized dataflow; this is a representation problem, not a runtime problem.
- Locking the format to one backend or one node implementation forever.

## Concepts

### Dataflow

A dataflow is a directed graph. Nodes fall into two broad classes:

- source/sink nodes, typically Kafka topics or ClickHouse tables
- processing nodes, such as extract-load, deduplication, or transformation stages

Edges represent how data moves between nodes. For example, a Kafka topic may feed an extract/load node, which then emits another Kafka topic or a ClickHouse table.

### Fragment

A fragment is a single JSON file representing a subset of the dataflow. Fragments are combined into a directory that represents the complete graph.

This allows a dataflow to be split by concern rather than forcing every definition into one giant blob. For example:

- one fragment for topic definitions
- one fragment for processing node definitions
- one fragment for edges and wiring

This layout is easy to diff, review, and extend.

## Serialization model

A serialized dataflow is stored as a directory containing one or more JSON fragments. There is no
manifest file: every `*.json` file directly inside the directory is a fragment, and all of them are
merged into one graph. The dataflow's name is simply the directory's own name — nothing declares it
explicitly.

Example layout:

```text
dataflow/
  topics.json
  nodes.json
  edges.json
```

Adding a new fragment is just adding a file; there's no separate listing to keep in sync with it.

Each fragment contains a JSON object keyed by identifier. The identifier is the stable name of the object within the graph.

## Fragment format

Each fragment is schema-driven JSON. The format should remain deliberately simple and map directly to runtime concepts.

Example fragment:

```json
{
  "nodes": {
    "kafka-source": {
      "type": "kafka-topic",
      "schema": {
        "id": "string",
        "ts": "timestamp[ms]",
        "amount": "int64"
      },
      "connection": {
        "broker": "localhost:9092"
      }
    },
    "dedup-process": {
      "type": "processing-node",
      "implementation": "tkati-node-dedup",
      "config": {
        "field": "id",
        "window_hours": 3
      }
    },
    "kafka-sink": {
      "type": "kafka-topic",
      "schema": {
        "id": "string",
        "ts": "timestamp[ms]",
        "amount": "int64"
      },
      "connection": {
        "broker": "localhost:9092"
      }
    }
  }
}
```

This is intentionally similar to the TOML config used by tkati nodes, but serialized in a portable structure that can be consumed by tools other than Python.

A fragment that describes a single node — e.g. one file per processing node, as a code generator
might produce — may instead declare it under a singular `node` key, with the id as a field on the
object itself rather than as the surrounding dict's key:

```json
{
  "node": {
    "id": "dedup-process",
    "type": "processing-node",
    "implementation": "tkati-node-dedup",
    "config": {
      "field": "id",
      "window_hours": 3
    }
  }
}
```

This is equivalent to a `"nodes": {"dedup-process": {...}}` entry (minus the `id` field, which
isn't part of the node's own metadata in that form) and merges into the same graph identically.

## Node model

At the serialized level, every node needs the following minimal metadata:

- `id`: stable node identifier
- `type`: node category, such as `kafka-topic`, `clickhouse-table`, or `processing-node`
- `name`: optional human-facing label
- `schema`: recommended for source/sink nodes, since it's what lets tooling validate wiring and render columns; optional everywhere, since it may not always be on hand (e.g. a node fragment written by hand, or discovered from a live cluster without introspecting its columns)
- `connection`: backend-specific connection settings
- `config`: node-specific runtime configuration
- optional `tags` or `metadata`: UI/tooling-friendly annotations, but not graph wiring

Graph wiring is intentionally stored outside the node definition in the `edges` fragment. Kafka consumer settings such as `group_id` belong to the incoming edge or input binding, because a single node may consume multiple streams with different consumer groups.

This structure allows the runtime and a visualization tool to understand the same graph without additional hidden context.

## Edge model

Edges are the relationships between nodes. They are serialized as separate objects or as a list under a dedicated `edges` fragment.

Example:

```json
{
  "edges": [
    {
      "from": "kafka-source",
      "to": "dedup-process",
      "kind": "stream",
      "consumer": {
        "group_id": "processing-node-group"
      }
    },
    {
      "from": "dedup-process",
      "to": "kafka-sink",
      "kind": "stream"
    }
  ]
}
```

This keeps the representation explicit and allows tools to render the graph cleanly even when node definitions are spread across multiple files. Consumer settings are carried on the inbound edge because different inputs to the same node may legitimately use different Kafka consumer groups.

## Why a directory of fragments

The directory model is chosen instead of a single monolithic JSON file for several reasons:

- easier diffs for review and code review
- smaller payloads for partial updates
- natural separation by concern (topics, tables, processing nodes, edges)
- easier extension when new node types or metadata are introduced
- support for incremental assembly from independent sources, where different teams or tools can contribute fragments without reserializing the whole graph
- no manifest to keep in sync — adding, removing, or renaming a fragment file is the whole change

A single-file format may be simpler at first, but a fragment directory scales better as the graph grows and becomes harder to inspect by hand.

The format intentionally allows the same node or edge definition to appear in multiple fragments as long as the definitions are equivalent. If the repeated definitions disagree, it is a validation error.

## Schema evolution

The format intentionally has no version stamp today — a dataflow directory is just its fragments,
with nothing declaring which revision of the format they were written for. Given the format's
small surface (a `nodes` map and an `edges` list, both open to unknown keys), backward-compatible
growth doesn't need one yet: a new optional field on a node or edge is safe for older tools to
ignore, and new node `type`s or edge `kind`s can be introduced without touching the loader.

Removing or repurposing an existing field would be a breaking change. If that's ever needed, a
version marker can be (re)introduced at that point — for example a well-known field on fragments
that carry it, or a small manifest file that tools fall back to looking for — rather than carrying
that machinery now for a compatibility problem the format doesn't have yet.

## Validation and tooling

The serialized form should be easy to validate with a JSON schema or a lightweight Pydantic model. At minimum, validation should check:

- the directory contains at least one JSON fragment
- node identifiers are unique
- repeated node or edge definitions across fragments must agree; otherwise validation fails
- edge references point to existing nodes
- a node's `schema`, when present, is a valid JSON object whose field types are recognized
- processing node configuration is structurally valid for that node type

This ensures the asset is useful not only to humans but also to other tooling.

## Example: a small dataflow

This could be the entire content of one fragment file in an `orders-pipeline/` directory, or split
across several as shown earlier (e.g. topics in one file, `edges` in another) — merging is
identical either way.

```json
{
  "nodes": {
    "raw-orders": {
      "type": "kafka-topic",
      "schema": {
        "id": "string",
        "time": "timestamp[ms]",
        "amount": "int64"
      },
      "connection": {
        "broker": "redpanda:29092"
      }
    },
    "dedup": {
      "type": "processing-node",
      "implementation": "tkati-node-dedup",
      "config": {
        "field": "id",
        "window_hours": 3
      }
    },
    "deduped-orders": {
      "type": "kafka-topic",
      "schema": {
        "id": "string",
        "time": "timestamp[ms]",
        "amount": "int64"
      },
      "connection": {
        "broker": "redpanda:29092"
      }
    }
  },
  "edges": [
    {
      "from": "raw-orders",
      "to": "dedup",
      "kind": "stream",
      "consumer": {
        "group_id": "orders-dedup"
      }
    },
    {"from": "dedup", "to": "deduped-orders", "kind": "stream"}
  ]
}
```

This is enough for a dashboard or metadata tool to render the topology and understand the lifecycle of the data as it moves through the system, while keeping wiring explicit and separate from node metadata.

## Future directions

This proposal intentionally keeps the format general enough to support future work without reworking the data model.

Possible extensions include:

- node metadata such as owners, tags, or lifecycle stages
- richer schema definitions for nested fields and arrays
- resource references for external stores and credentials
- pinning to deployment environments or clusters
- a richer graph format with labels, annotations, and rendering hints

The important point is that the serialized graph is a persistent, portable description of the topology, not a shadow of the process state.

## Recommendation

Serialize a tkati dataflow as a directory of JSON fragments representing a graph of nodes and edges — every JSON file in the directory is a fragment, with no manifest to keep in sync. Keep the model explicit, schema-aware, and easy to inspect. This gives us a practical representation for visualization, tooling, and future tooling integrations without coupling the design to any one runtime implementation.
