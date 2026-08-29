# tkati-dashboard — dataflow graph viewer

Reads a serialized tkati dataflow directory (see
[docs/dataflow-serialization.md](../../docs/dataflow-serialization.md)) and serves a local web page
rendering it as a graph — no live runtime process required, no manifest to maintain, just a
directory of `*.json` fragment files.

## Usage

```sh
tkati-dashboard path/to/dataflow-dir
```

Then open `http://127.0.0.1:8000/` in a browser. The page fetches `/api/graph` and renders it
top-to-bottom with [React Flow](https://reactflow.dev); source/sink nodes (`kafka-topic`,
`clickhouse-table`) and processing nodes are colored differently, and stream edges are labeled by
`kind` plus, for a Kafka consumer edge, its `group_id` and live lag. Click a node to open a side
panel with its full connection/config/schema details.

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

Clicking a node opens a side panel with its full metadata: connection settings, `config`, and
`schema` (field → type). For a `kafka-topic` node, the panel also fetches
`GET /api/nodes/{id}/snapshot`, which connects live to `connection.broker`/`connection.topic` and
shows the most recent messages on that topic (newest last), parsed as JSON. This is one of two
places `tkati-dashboard` talks to a live broker rather than just the serialized directory — it's a
best-effort convenience for the panel, not something the graph view itself depends on: a broker
that's unreachable, or a topic that doesn't exist, shows an inline error in that section instead of
breaking the page. It uses a throwaway consumer group and never commits offsets, so it never
interferes with a real pipeline's consumers.

## Consumer lag

For every stream edge whose `consumer.group_id` is set and whose source is a `kafka-topic`, the
page also fetches `GET /api/nodes/{topic_id}/consumer-lag?group_id=...` and appends the result to
the edge's label (e.g. `stream · group: orders-dedup · lag: 4`). This is the other place
`tkati-dashboard` talks to a live broker: it looks up `group_id`'s committed offset with
`Consumer.committed()` and compares it to the topic's high watermark — it never subscribes or
polls as that group, so it can't join it, trigger a rebalance, or otherwise disturb a real
pipeline's consumer. A group that has never committed an offset is reported as fully behind (lag
= the topic's full size); an unreachable broker shows `lag: n/a` on that edge instead of failing
the page.

## Validation

`tkati_dashboard.dataflow.load_dataflow` enforces the rules from the serialization doc: the
directory must contain at least one `*.json` fragment, node ids must be unique (or identically
redefined) across fragments, edges must reference existing nodes, and source/sink schemas must use
field types known to `tkati_core.type_mapping`. A validation failure surfaces as an HTTP 422 with
the error message, shown inline on the page instead of a blank graph.
