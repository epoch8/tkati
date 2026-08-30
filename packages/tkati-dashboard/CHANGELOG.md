# 0.4.0

* New package `tkati-dashboard`: a local web server that reads one or more serialized tkati
  dataflow directories (a directory of `*.json`/`*.yaml`/`*.yml` fragments, freely mixable, no
  manifest required — see
  [docs/dataflow-serialization.md](../../docs/dataflow-serialization.md)) and serves a page
  rendering each as an interactive graph (React Flow, laid out left-to-right by
  [dagre](https://github.com/dagrejs/dagre) using each node's real measured size, so a long
  `broker`/`topic` string pushes neighbors aside instead of overlapping them). Source/sink nodes
  (`kafka-topic`, `clickhouse-table`) and processing nodes are colored differently; a dataflow's
  name is its directory's own name. Nodes can be declared via a `"nodes": {<id>: {...}}` dict or
  one-per-fragment via a top-level `"node"` object carrying its own `"id"`.
* Observe multiple flows from one dashboard instance: pass more than one directory on the command
  line, and/or `--flows-root DIR` to auto-discover every fragment-containing subdirectory of
  `DIR` as its own flow (rescanned live, so adding/removing one is picked up without restarting).
  A ☰ menu in the graph's top-left corner switches between flows when there's more than one
  (persisted, and reflected in the URL as `?flow=<id>` for bookmarking/sharing); with just one
  flow the page has no picker at all.
* Click a node to fill the always-visible, resizable inspector panel (drag its left edge) with
  its full `connection`, `config`, and `schema` — `schema` is optional for every node type, and
  its field types are validated against `tkati_core.type_mapping` when present. `connection` and
  `schema` render as tables; `config` renders as a card per entry (its key as a header, its value
  as a pretty-printed code block). Every section is independently collapsible without losing
  already-fetched state.
* A `kafka-topic` node's inspector also fetches, live from the broker (best-effort — an
  unreachable broker shows an inline error instead of breaking the page): its most recent
  messages ("Latest events", fetched on demand and refreshable, each shown as its own
  pretty-printed JSON block with Kafka `partition`/`offset`/timestamp), via a throwaway consumer
  group that never commits offsets; and its partitioning, replication, and topic-level config
  ("Topic stats", flagging any under-replicated partition and any config value that differs from
  the broker default).
* An incoming stream edge with a `consumer.group_id` shows as its own row stacked inside the
  *consuming* node — colored a deeper shade of the node's own color — with its `group` and live
  `lag`, and its arrow landing directly on that row; an edge with nothing extra to show stays a
  plain, unlabeled-beyond-`kind` line into the node. Lag is kept fresh with a Grafana-style
  control in the graph's top-right corner (a "↻" button to refresh every visible edge's lag
  immediately, and a dropdown for an auto-refresh interval — Off/5s/15s/30s/1m/5m, persisted,
  paused while the tab is hidden) plus a per-row "↻" in the inspector's "Consumer lag" section to
  refresh just one edge. Lag is read via committed offsets (`Consumer.committed()`), never
  subscribing or polling as that group.
* Selecting a node glows its box blue and highlights every edge into or out of it in the graph in
  the same color, drawn above any edge it crosses.
* Added `examples/simple-pipeline` (two topics, one dedup node) with a `seed_kafka.py` script
  that populates it with sample events, including intentional duplicates, and a bigger
  `examples/analytics-pipeline` exercising fragment merging across four files.
