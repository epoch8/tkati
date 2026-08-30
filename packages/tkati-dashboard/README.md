# tkati-dashboard — dataflow graph viewer

Reads a serialized tkati dataflow directory (see
[docs/dataflow-serialization.md](../../docs/dataflow-serialization.md)) and serves a local web page
rendering it as a graph — no live runtime process required, no manifest to maintain, just a
directory of `*.json`/`*.yaml`/`*.yml` fragment files (freely mixable — both encodings merge into
the same graph identically).

## Usage

```sh
tkati-dashboard path/to/dataflow-dir
```

Then open `http://127.0.0.1:8000/` in a browser. The page fetches `/api/graph` and renders it
top-to-bottom with [React Flow](https://reactflow.dev); source/sink nodes (`kafka-topic`,
`clickhouse-table`) and processing nodes are colored differently, and stream edges are labeled by
`kind` plus, for a Kafka consumer edge, its `group_id` and live lag. Click a node to fill the
always-visible inspector panel on the right with its full connection/config/schema details; drag
its left edge to resize it (the width is remembered across reloads).

### Try it with the bundled example

[`examples/simple-pipeline`](examples/simple-pipeline) is the smallest interesting dataflow: two
Kafka topics and one processing node (`raw-events` → `dedup` → `deduped-events`). Its `connection`
blocks point at `localhost:9092`, so if you have a broker there, seed it with sample events first:

```sh
uv run python packages/tkati-dashboard/examples/simple-pipeline/seed_kafka.py
uv run tkati-dashboard packages/tkati-dashboard/examples/simple-pipeline
```

Then open <http://127.0.0.1:8000/> and click the `raw-events` or `deduped-events` node — the
"Latest events" section in the side panel shows the real messages the seed script just produced
(`raw-events` includes two intentional duplicate `event_id`s so you can see what the `dedup` node
in between is for).

For a bigger graph exercising fragment merging across `topics.json`, `tables.json`, `nodes.json`,
and `edges.json` (two raw topics → dedup → a sessionize/enrich node → a ClickHouse table, plus a
side branch straight to another table), see [`examples/analytics-pipeline`](examples/analytics-pipeline)
— its topics aren't seeded with data, so "Latest events" there will error unless you produce to
them yourself.

Options:

- `--host` (default `127.0.0.1`)
- `--port` (default `8000`)

The dataflow directory is re-read on every request to `/api/graph`, so editing the fragments and
refreshing the browser picks up the change without restarting the server.

## Node panel

The panel on the right is always there, like an inspector rather than a popup — it shows a
placeholder until you click a node, then shows that node's full metadata: connection settings,
`config`, and `schema` (field → type). Clicking empty canvas clears the selection back to the
placeholder. Every section has a clickable header to collapse/expand it — a collapsed section
stays mounted, just hidden, so collapsing a live-fetching section and reopening it doesn't
re-fetch. For a `kafka-topic` node, the panel also fetches two more, live views:

- `GET /api/nodes/{id}/snapshot` connects to `connection.broker`/`connection.topic` and shows the
  most recent messages on that topic (newest last), parsed as JSON, using a throwaway consumer
  group that never commits offsets. Each message renders as its own pretty-printed JSON block
  (fields reordered to match `schema`, when there is one) rather than a table — real messages
  commonly carry 5-20 fields, too many to lay out sensibly as table columns in a side panel.
- `GET /api/nodes/{id}/topic-stats` shows the topic's partitioning and replication (per-partition
  leader/replicas/in-sync-replicas, flagging any under-replicated partition) and its topic-level
  config — `retention.ms`, `retention.bytes`, `cleanup.policy`, `segment.bytes`,
  `compression.type`, `max.message.bytes` — via `tkati_dashboard.topic_stats`, marking each value
  that differs from the broker default. Partition/replica info comes from the same topic metadata
  lookup as the snapshot/lag features (`_kafka_metadata.py`); the config comes from a separate
  read-only `AdminClient.describe_configs()` call.

These are two of the places `tkati-dashboard` talks to a live broker rather than just the
serialized directory — best-effort conveniences for the panel, not something the graph view itself
depends on: a broker that's unreachable, or a topic that doesn't exist, shows an inline error in
that section instead of breaking the page.

## Consumer lag

For every stream edge whose `consumer.group_id` is set and whose source is a `kafka-topic`, the
page also fetches `GET /api/nodes/{topic_id}/consumer-lag?group_id=...` and appends the result to
the edge's label (e.g. `stream · group: orders-dedup · lag: 4`). This is another place
`tkati-dashboard` talks to a live broker: it looks up `group_id`'s committed offset with
`Consumer.committed()` and compares it to the topic's high watermark — it never subscribes or
polls as that group, so it can't join it, trigger a rebalance, or otherwise disturb a real
pipeline's consumer. A group that has never committed an offset is reported as fully behind (lag
= the topic's full size); an unreachable broker shows `lag: n/a` on that edge instead of failing
the page.

## Validation

`tkati_dashboard.dataflow.load_dataflow` enforces the rules from the serialization doc: the
directory must contain at least one `*.json`/`*.yaml`/`*.yml` fragment, node ids must be unique
(or identically redefined) across fragments, edges must reference existing nodes, and a node's `schema`, when
present, must use field types known to `tkati_core.type_mapping` — `schema` itself is always
optional, since it isn't always on hand for a real-world node. A validation failure surfaces as
an HTTP 422 with the error message, shown inline on the page instead of a blank graph.
