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

Then open `http://127.0.0.1:8000/` in a browser. Pass more than one directory (or
`--flows-root`, see [Multiple flows](#multiple-flows) below) to observe several dataflows from
one dashboard instead of just this one. The page fetches the list of flows from `/api/flows`,
then the selected one's graph from `/api/flows/{id}/graph`, and renders it with
[React Flow](https://reactflow.dev), laid out by [dagre](https://github.com/dagrejs/dagre) using
each node's real measured box (from its label text, via canvas measurement — no DOM mount needed)
rather than a flat grid, so a node with a long `broker`/`topic` string pushes its neighbors aside
instead of overlapping them. The graph always lays out left-to-right.

A node consuming a stream with a consumer group doesn't get that edge's `group`/`lag` as a
floating label — it gets its own stacked row inside the *consuming* node instead, right below the
node's own header, one row per such input (so a fan-in node like a sessionizer joining two topics
shows two rows, each fed by its own arrow landing directly on its row). An edge with nothing extra
to show beyond its `kind` (no consumer group) stays a plain, unlabeled-beyond-`kind` line into the
node's header. Source/sink nodes (`kafka-topic`, `clickhouse-table`) and processing nodes are
colored differently. Click a node to fill the always-visible inspector panel on the right — the
whole node box glows blue while selected — with its full connection/config/schema details, plus
every stream edge touching it and that edge's live consumer lag; drag the panel's left edge to
resize it (the width is remembered across reloads).

### Try it with the bundled example

[`examples/simple-pipeline`](examples/simple-pipeline) is the smallest interesting dataflow: two
Kafka topics and one processing node (`raw-events` → `dedup` → `deduped-events`). Its `connection`
blocks point at `localhost:9092`, so if you have a broker there, seed it with sample events first:

```sh
uv run python packages/tkati-dashboard/examples/simple-pipeline/seed_kafka.py
uv run tkati-dashboard packages/tkati-dashboard/examples/simple-pipeline
```

Then open <http://127.0.0.1:8000/>, click the `raw-events` or `deduped-events` node, and click
"Load latest events" in the side panel to see the real messages the seed script just produced
(`raw-events` includes two intentional duplicate `event_id`s so you can see what the `dedup` node
in between is for).

For a bigger graph exercising fragment merging across `topics.json`, `tables.json`, `nodes.json`,
and `edges.json` (two raw topics → dedup → a sessionize/enrich node → a ClickHouse table, plus a
side branch straight to another table), see [`examples/analytics-pipeline`](examples/analytics-pipeline)
— its topics aren't seeded with data, so "Latest events" there will error unless you produce to
them yourself.

Since `examples/` itself contains both as sibling directories, it also doubles as a ready-made
`--flows-root` demo — `tkati-dashboard --flows-root packages/tkati-dashboard/examples` serves both
as separate flows, switchable from the ☰ menu (see [Multiple flows](#multiple-flows)).

Options:

- `--host` (default `127.0.0.1`)
- `--port` (default `8000`)
- `--flows-root DIR` — auto-discover flows (see below); repeatable

A dataflow directory is re-read on every request to its `/api/flows/{id}/graph`, so editing the
fragments and refreshing the browser picks up the change without restarting the server.

## Multiple flows

A real deployment usually runs more than one dataflow per environment, and one dashboard
instance can observe all of them at once instead of one directory per instance:

```sh
# name each flow directory explicitly (flow id = each directory's own basename)
tkati-dashboard path/to/orders-flow path/to/clicks-flow

# or point at a parent directory and let every fragment-containing subdirectory become a flow
tkati-dashboard --flows-root path/to/env/flows
```

The two forms can be combined, and `--flows-root` is repeatable. Either way, a flow's id is its
directory's own basename — passing two directories with the same name is a startup error asking
you to rename one of them. Unlike the explicit directories, a `--flows-root`'s subdirectories are
rescanned on every request to `/api/flows`, so adding or removing a flow directory there shows up
without restarting the server; a subdirectory with no fragments in it (a stray `README`, say) is
silently skipped rather than treated as an error.

With more than one flow configured, the graph page gets a ☰ menu in its top-left corner naming the
currently selected flow — click it to switch to another one. It stays collapsed to that single
button otherwise, so the canvas keeps its space once a flow is picked.
The current flow is also reflected in the page's URL as `?flow=<id>`, so a link to a specific
flow's view can be bookmarked or shared. With only one flow configured (the common case, and the
default if you pass a single directory as before), the menu doesn't appear at all — the page looks
exactly as it always has.

## Node panel

The panel on the right is always there, like an inspector rather than a popup — it shows a
placeholder until you click a node, then shows that node's full metadata: connection settings,
`config`, and `schema` (field → type). Clicking empty canvas clears the selection back to the
placeholder. Every section has a clickable header to collapse/expand it — a collapsed section
stays mounted, just hidden, so collapsing a live-fetching section and reopening it doesn't
re-fetch. For a `kafka-topic` node, the panel also fetches two more, live views:

- `GET /api/flows/{flow_id}/nodes/{id}/snapshot` connects to `connection.broker`/`connection.topic` and shows the
  most recent messages on that topic (newest last), parsed as JSON, using a throwaway consumer
  group that never commits offsets. It only fetches on demand — click "Load latest events" — and
  a "↻ Refresh" button afterward pulls a fresh batch on request rather than automatically. Each
  message renders as its own pretty-printed JSON block (fields reordered to match `schema`, when
  there is one) rather than a table — real messages commonly carry 5-20 fields, too many to lay
  out sensibly as table columns in a side panel — with a small header showing that message's
  Kafka-level `partition`, `offset`, and timestamp, alongside its parsed body.
- `GET /api/flows/{flow_id}/nodes/{id}/topic-stats` shows the topic's partitioning and replication (per-partition
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
page also fetches `GET /api/flows/{flow_id}/nodes/{topic_id}/consumer-lag?group_id=...` and shows the result in
two places: the `group`/`lag` lines in that edge's stacked row inside the consuming node (see
above), and, for either node that edge touches, the inspector panel's "Consumer lag" section
(`← other-node (group_id)` for an edge consumed by the selected node, `→ other-node (group_id)`
for one where it's the topic being consumed). This is another place `tkati-dashboard` talks to a
live broker: it looks up `group_id`'s committed offset with `Consumer.committed()` and compares
it to the topic's high watermark — it never subscribes or polls as that group, so it can't join
it, trigger a rebalance, or otherwise disturb a real pipeline's consumer. A group that has never
committed an offset is reported as fully behind (lag = the topic's full size); an unreachable
broker shows `lag: n/a` instead of failing the page.

Lag is time-sensitive, so it doesn't just get fetched once. A ☰-style control in the canvas's
top-right corner (shown whenever the graph has at least one such edge) works like Grafana's
refresh picker: a "↻" button re-fetches every visible edge's lag immediately, and a dropdown next
to it sets an auto-refresh interval (Off/5s/15s/30s/1m/5m, persisted across reloads, paused while
the browser tab isn't visible). The inspector's "Consumer lag" section additionally has its own
per-row "↻" to refresh just the one edge you're looking at, without waiting for the next tick or
refreshing every other edge in the graph.

## Validation

`tkati_dashboard.dataflow.load_dataflow` enforces the rules from the serialization doc: the
directory must contain at least one `*.json`/`*.yaml`/`*.yml` fragment, node ids must be unique
(or identically redefined) across fragments, edges must reference existing nodes, and a node's `schema`, when
present, must use field types known to `tkati_core.type_mapping` — `schema` itself is always
optional, since it isn't always on hand for a real-world node. A validation failure surfaces as
an HTTP 422 with the error message, shown inline on the page instead of a blank graph.
