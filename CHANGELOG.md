# Changelog

One entry per jj change, keyed by its change identifier (stable across
`jj describe`/`jj squash`/rebases — use `jj log -r <change-id>` to look one
up). Newest first.

## WIP 0.4.0

### nlmmssxp — tkati-dashboard: lazy-load latest events, with Kafka offset/timestamp metadata

- `tkati_dashboard.snapshot.fetch_kafka_snapshot` now returns
  `{partition, offset, timestamp, timestamp_type, value}` per message instead of just the
  parsed `value` — Kafka-level metadata alongside the message body, since the two aren't
  otherwise distinguishable once decoded. `timestamp` is `None` (`timestamp_type`
  `"unavailable"`) when the broker didn't record one.
- "Latest events" no longer fetches automatically when a `kafka-topic` node is selected — it
  shows a "Load latest events" button instead, plus a "↻ Refresh" button once loaded, so
  selecting a node doesn't unconditionally hit the broker. Switching to a different node drops
  any previously loaded batch back to the button rather than leaving stale events up under a
  "Refresh" that would silently apply to the wrong topic.
- Each message now shows a small header with its `partition`, `offset`, and timestamp above
  its pretty-printed JSON body.
- Fixed pressing "↻ Refresh" resetting the inspector panel's scroll position: the previous
  event list was replaced with a bare "Loading…" while the refresh was in flight, which
  shrank the scrollable panel and made the browser clamp its scroll offset. The last loaded
  batch now stays rendered (with the button relabeled "Refreshing…" and disabled) until the
  new one lands, so the panel's height — and scroll position — never collapses mid-refresh.

### suonpnzk — tkati-dashboard: always-visible resizable inspector, message-oriented events

- The node side panel is now an always-visible inspector rather than a popup: it shows a
  placeholder ("Select a node to inspect it") until a node is clicked, instead of only existing
  while one is selected. Dropped its close button accordingly — clicking empty canvas already
  clears the selection back to the placeholder.
- The panel is now resizable via a drag handle on its left edge
  (`useResizablePanelWidth` in `static/index.html`), clamped to 260-800px and persisted to
  `localStorage` across reloads.
- `EventSnapshot`'s "Latest events" no longer renders a table: a real message commonly has
  5-20 fields, too many to lay out as table columns in a side panel. Each message now renders
  as its own pretty-printed JSON block instead (`JSON.stringify(event, null, 2)`, fields
  reordered to match `schema` first when there is one).
- Every panel section (Connection, Config, Schema, Topic stats, Latest events) is now
  collapsible — click its header to toggle. A collapsed section's content stays mounted (just
  hidden), so a live-fetching section like Topic stats or Latest events doesn't re-fetch every
  time it's reopened.

### uznzxwzx — docs: connection fields; tkati-dashboard: read YAML fragments too

- [docs/dataflow-serialization.md](docs/dataflow-serialization.md) never actually specified how
  to declare a Kafka topic's physical name — every example only showed `connection.broker`,
  leaving a reader to (wrongly) assume the node's own `id` doubles as the topic name. Added a
  "Connection settings" subsection under "Node model" spelling out `kafka-topic`'s `broker`/
  `topic` (matching what `tkati-dashboard`'s live lookups actually read) and
  `clickhouse-table`'s `host`/`port`/`database`/`user`/`secure`, plus a note against putting
  live credentials in a checked-in fragment.
- Updated every example fragment in the doc to include `connection.topic`, and noted in
  "Validation and tooling" that these per-type connection fields are not enforced by
  `load_dataflow` — a node missing them still loads, it just can't back a live lookup.
- `tkati_dashboard.dataflow.load_dataflow` now reads `*.yaml`/`*.yml` fragments alongside
  `*.json` ones in the same directory — both decode to the same nodes/edges structure before
  the existing merge/validation pipeline ever sees them, so JSON and YAML fragments merge and
  dedupe identically (including a node defined identically in one of each). New
  `find_fragment_paths()` (shared with `main.py`'s CLI sanity check) replaces the old
  JSON-only glob. Added a matching YAML example to the doc's "Fragment format" section.

### qoorwymz — tkati-dashboard: support a singular "node" fragment shape

- Fixed `load_dataflow` failing to resolve edges to a node declared via a top-level `"node"`
  object with its own `"id"` field (e.g. one file per processing node, as a code generator
  might produce), instead of the usual `"nodes": {<id>: {...}}` dict — it was silently
  ignored, so any edge referencing it raised "references unknown node".
- Extracted `_merge_node`, shared by both the `"nodes"` dict and singular `"node"` code paths.
- Documented the singular `"node"` shape in
  [docs/dataflow-serialization.md](docs/dataflow-serialization.md)'s "Fragment format" section.

### ykrrxmkz — tkati-dashboard: topic stats in the node panel, optional schema

- New `tkati_dashboard.topic_stats.fetch_topic_stats` and `GET
  /api/nodes/{id}/topic-stats`: per-partition leader/replicas/in-sync-replicas
  (flagging any under-replicated partition) from the same topic metadata
  lookup as `snapshot.py`/`lag.py`, plus topic-level config
  (`retention.ms`, `retention.bytes`, `cleanup.policy`, `segment.bytes`,
  `compression.type`, `max.message.bytes`) via a separate read-only
  `AdminClient.describe_configs()` call, each entry flagged if it differs
  from the broker default.
- The node panel shows this as a new "Topic stats" section for
  `kafka-topic` nodes, alongside "Latest events"; degrades to an inline
  error, like the other live lookups, if the broker is unreachable.
- `_kafka_metadata.py`: extracted `resolve_topic_metadata` (full
  partition metadata) with `resolve_partitions` now built on top of it.
- Fixed `load_dataflow` rejecting a real-world fragment (e.g. a
  `clickhouse-table` node hand-written or discovered from a live cluster
  without introspecting its columns) whose source/sink node has no
  `schema` — `Node '...' of type '...' needs a schema`. `schema` is now
  optional for every node type; when present, its field types are still
  validated against `tkati_core.type_mapping`. Updated
  [docs/dataflow-serialization.md](docs/dataflow-serialization.md)'s node
  model and validation sections to match: `schema` is recommended, not
  required.

### zpwmtxrn — Add tkati-dashboard: dataflow graph viewer

- New package `tkati-dashboard`: a local web server that reads a serialized
  tkati dataflow directory — a directory of JSON fragments, no manifest
  required; every `*.json` file directly inside it is merged into the graph
  (per [docs/dataflow-serialization.md](docs/dataflow-serialization.md)) —
  and renders it as a top-to-bottom graph in the browser (React Flow), the
  doc's stated visualization-dashboard use case.
- `tkati_dashboard.dataflow.load_dataflow` merges fragments and enforces the
  doc's validation rules: the directory has at least one fragment,
  unique-or-identical node ids across fragments, edges referencing existing
  nodes, and source/sink schema field types validated against the existing
  `tkati_core.type_mapping` vocabulary. A dataflow's name is its directory's
  own name.
- `GET /api/graph` re-reads the directory on every request, so editing the
  dataflow and refreshing the page picks up the change without restarting
  the server. Nodes show `broker`/`topic` (for `kafka-topic`) and edges show
  their consumer `group_id` when set.
- Clicking a node opens a side panel with its full `connection`, `config`,
  and `schema`. For a `kafka-topic` node, the panel also fetches
  `GET /api/nodes/{id}/snapshot` (`tkati_dashboard.snapshot`), which connects
  live to the topic's broker and shows its most recent messages using a
  throwaway consumer group that never commits offsets.
- Every stream edge with a `consumer.group_id` whose source is a
  `kafka-topic` fetches and shows its live consumer lag in the edge label
  (`stream · group: orders-dedup · lag: 4`), via
  `GET /api/nodes/{id}/consumer-lag` and `tkati_dashboard.lag`, which reads
  committed offsets with `Consumer.committed()` — it never subscribes or
  polls as that group, so it can't join it or trigger a rebalance.
  `_kafka_metadata.resolve_partitions` is shared between `snapshot.py` and
  `lag.py`. Both live-broker lookups degrade to an inline error (or
  `lag: n/a`) instead of breaking the page when the broker or topic is
  unreachable.
- New `examples/simple-pipeline` (two Kafka topics either side of a
  `tkati-node-dedup` node) with a `seed_kafka.py` script that populates it
  with sample events, including intentional duplicates to make the dedup
  node's purpose visible; and a bigger `examples/analytics-pipeline`
  exercising fragment merging across four files.

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
