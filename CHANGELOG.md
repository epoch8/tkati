# Changelog

One entry per jj change, keyed by its change identifier (stable across
`jj describe`/`jj squash`/rebases — use `jj log -r <change-id>` to look one
up). Newest first.

## 0.4.0

### mtxpxzkt — Add tkati-dashboard: multi-flow dataflow graph viewer

- New package `tkati-dashboard`: a FastAPI server (`app.py`) plus a single-file React
  18/ReactFlow v11 frontend (`static/index.html`, no build step, ESM imports from esm.sh) that
  reads one or more serialized tkati dataflow directories (`dataflow.py`'s `load_dataflow`, per
  [docs/dataflow-serialization.md](docs/dataflow-serialization.md): a directory of
  `*.json`/`*.yaml`/`*.yml` fragments, freely mixable, no manifest, merged via a `"nodes"` dict or
  a singular top-level `"node"` object) and renders each as a graph laid out left-to-right by
  `dagre`, sized from each node's real measured label box rather than a flat grid.
  `load_dataflow` enforces unique-or-identical node ids across fragments, edges referencing
  existing nodes, and (optional) `schema` field types against `tkati_core.type_mapping`; a
  dataflow's name is its directory's own name.
- Multi-flow (`flows.py`, `main.py`, `app.py`): the CLI takes one or more `dataflow_dir`
  positional args and/or a repeatable `--flows-root DIR` (auto-discovering every
  fragment-containing subdirectory as its own flow, rescanned per request). Every route is
  nested under `/api/flows/{flow_id}/...`, plus `GET /api/flows` listing them. The frontend's
  `FlowMenu` (a collapsed ☰ button, shown only with more than one flow) switches between them,
  persisted and reflected in the URL as `?flow=<id>` for bookmarking/sharing.
- Node inspector (`InspectorPanel`/`NodeDetails`): an always-visible, resizable side panel with
  independently collapsible `Connection`/`Config`/`Schema` sections — `Config` renders each entry
  as a card (key as header, pretty-printed value as a code block) via `ConfigCards`. A
  `kafka-topic` node additionally fetches, live and on demand, its most recent messages
  (`GET .../nodes/{id}/snapshot`, `snapshot.py`, a throwaway non-committing consumer group) and
  topic stats — partitioning/replication/config (`GET .../topic-stats`, `topic_stats.py`,
  flagging under-replication and any value differing from the broker default). Both degrade to an
  inline error instead of breaking the page if the broker is unreachable.
- Consumer lag (`lag.py`, `GET .../consumer-lag`, reading committed offsets via
  `Consumer.committed()`, never subscribing/polling as that group): an incoming edge naming a
  `consumer.group_id` renders as its own row stacked inside the *consuming* node
  (`StackedNode`/`layout()`, colored a deeper shade of the node's own color), with its arrow
  landing directly on that row via a named `Handle`; an edge with nothing extra to show stays a
  plain `kind`-only line. Lag is kept fresh via a Grafana-style `RefreshControl` in the graph's
  top-right corner (a manual "↻" plus an auto-refresh interval — Off/5s/15s/30s/1m/5m, persisted,
  paused while the tab is hidden, all backed by one shared `fetchLag` callback) and a per-row "↻"
  in the inspector's "Consumer lag" section.
- Selecting a node glows it blue and highlights every edge touching it the same color
  (`SELECTED_EDGE_COLOR`), drawn above any edge it crosses.
- `examples/simple-pipeline` (two Kafka topics either side of a dedup node, with a
  `seed_kafka.py` script seeding sample events including intentional duplicates) and a bigger
  `examples/analytics-pipeline` exercising fragment merging across four files.

## 0.3.1

### xwxxmoms — Preserve timestamp[ms] through Kafka JSON round trip

- Fixed `KafkaProducer`'s JSON format silently turning `timestamp[ms]`
  columns into ISO-8601 strings instead of the original epoch-ms int,
  breaking noop consumer→producer round trips (e.g. `tkati-node-el`).
- `KafkaProducer` now accepts an optional output schema (the topic's
  existing `schema` setting) and casts declared columns back to their wire
  type before serializing, symmetric with how `KafkaConsumer` parses them
  in. An unrecognized schema type now raises instead of silently
  defaulting to string.
- Extracted the type-string → pyarrow-type mapping into a shared
  `tkati_core.type_mapping` module used by both the consumer and producer.
- `tkati-node-dedup`'s output topic now declares a `schema` (`settings.test.toml`
  and its test fixtures) so it also benefits from the fix — its `time` field
  round-trips as the original epoch-ms int again.

## 0.3.0

Upgrading from v0.2.0? See [MIGRATION.md](MIGRATION.md) for the full guide
(settings restructuring, renamed parameters, and the two new node packages).

### twqstomm — Add tkati-node-dedup: Kafka-to-Kafka streaming dedup node

- New package `tkati-node-dedup`: reads a Kafka topic and republishes it
  deduplicated by a configurable field over a rolling processing-time window
  (e.g. dedup by uid on a 3 hour window).
- Dedup state lives in an embedded RocksDB store partitioned into hourly
  buckets that are auto-cleaned once they age out of the window.
- At-least-once delivery: a key is only marked seen after a confirmed,
  flushed produce, and the input offset is only committed after that — on
  any crash or dedup-store failure the node prefers forwarding a possible
  duplicate over dropping a real event.
- Store lookups and writes are batched against RocksDB (list-based `get()`,
  `WriteBatch`) rather than one call per row, with `pyarrow.compute` used
  for vectorized key encoding, to keep per-batch overhead flat at high
  message volumes.
