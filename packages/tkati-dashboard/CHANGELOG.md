# 0.4.0a13

* Initial implementation of `tkati-dashboard`: reads a serialized tkati dataflow directory — a
  directory of JSON fragments, no manifest required, every `*.json` file directly inside it is
  merged into the graph — and serves a local web page rendering it top-to-bottom as a graph with
  React Flow, colored by source/sink vs. processing node. A dataflow's name is its directory's
  own name.
* The graph shows `broker`/`topic` on `kafka-topic` nodes and the consumer `group_id` (plus live
  lag, see below) on edges that carry one
* Clicking a node opens a side panel with its full connection/config/schema. For a `kafka-topic`
  node, the panel also fetches a live snapshot of the topic's most recent messages via
  `GET /api/nodes/{id}/snapshot` and `tkati_dashboard.snapshot` (a throwaway consumer group,
  never commits offsets)
* Stream edges with a `consumer.group_id` show their live consumer lag, via
  `GET /api/nodes/{id}/consumer-lag` and `tkati_dashboard.lag` (reads committed offsets with
  `Consumer.committed()`, never subscribes/polls as the group). Both live-broker lookups show an
  inline error instead of breaking the page when the broker/topic is unreachable
* Added `examples/simple-pipeline` (two topics, one dedup node) with a `seed_kafka.py` script
  that populates it with sample events, including intentional duplicates, and a bigger
  `examples/analytics-pipeline` exercising fragment merging across four files
* The node panel's `kafka-topic` section now also shows topic stats: per-partition
  leader/replicas/in-sync-replicas (flagging under-replication) and topic config
  (`retention.ms`, `cleanup.policy`, etc., each flagged when it differs from the broker
  default), via `GET /api/nodes/{id}/topic-stats` and `tkati_dashboard.topic_stats`
* `schema` is optional for every node type (previously required for `kafka-topic`/
  `clickhouse-table`), since it isn't always on hand for a real-world node
* A fragment can now also declare a single node via a top-level `"node"` object carrying its
  own `"id"`, instead of only `"nodes": {<id>: {...}}` — fixes edges to such a node being
  reported as referencing an unknown node
* `load_dataflow` now also reads `*.yaml`/`*.yml` fragments, freely mixable with `*.json` ones
  in the same directory — both decode to the same nodes/edges structure and merge identically
* The node panel is now an always-visible, resizable inspector (drag its left edge; width
  persisted to `localStorage`) instead of a popup that only exists while a node is selected
* "Latest events" now shows each message as its own pretty-printed JSON block instead of a
  table, since a real message commonly has too many fields for table columns to work in a
  narrow panel
* Every panel section (Connection, Config, Schema, Topic stats, Latest events) is now
  collapsible, without unmounting/re-fetching a live-fetching section when reopened
* "Latest events" is now fetched on demand via a "Load latest events" button, with a
  "↻ Refresh" button once loaded, rather than automatically on node selection
* Each event returned by `fetch_kafka_snapshot`/`GET /api/nodes/{id}/snapshot` now carries its
  Kafka `partition`, `offset`, and `timestamp` alongside the parsed message body (`value`),
  shown as a small header above each message in the panel
* Fixed "↻ Refresh" resetting the inspector panel's scroll position — the previous events now
  stay rendered while a refresh is in flight instead of being replaced by a "Loading…" that
  briefly shrinks the scrollable panel
* The graph is now laid out by dagre using each node's real measured size instead of a flat
  grid, so a node with a long `broker`/`topic` string no longer overlaps its neighbors
* Added an LR/TD layout toggle (LR now the default), persisted across reloads
* The selected node now shows a blue border/glow in the graph itself, not just in the panel
* Edge labels are now multi-line (`kind`/`group:`/`lag:`, one per line) via a custom edge type,
  instead of a single `·`-joined line
* The inspector panel has a new "Consumer lag" section listing every stream edge touching the
  selected node and its live lag
* Fixed all graph nodes vanishing while dragging the inspector panel's resize handle — the
  graph layout is now memoized instead of being recomputed on every resize-driven re-render
* Fixed a selected node's text overflowing past its bottom border (visible with a label line
  close to the box's measured width) — the selection highlight now only changes the border's
  color, not its width, since the box's size assumes a constant border width
* An incoming edge with a consumer group now shows its `group`/`lag` as a stacked row inside
  the consuming node instead of a floating edge label, with the arrow landing directly on that
  row (a new `StackedNode` custom node type); an edge with nothing extra to show keeps a plain
  `kind`-only edge label
* Fixed the header/row sections' own square-cornered borders getting chopped instead of
  following the node's rounded outer corners
* Fixed a row-targeting edge's arrow landing in empty space past the node instead of on its
  row: a ReactFlow "dynamic handles" gotcha (fixed via `useUpdateNodeInternals`) plus a row
  Handle's position being computed relative to the wrong parent (the whole node instead of
  just that row's own div, which is what it's actually rendered inside)
* A row's color is now a deeper shade of its own node's color instead of a fixed indigo
* One dashboard instance can now observe multiple flows: pass more than one `dataflow_dir` on the
  command line, and/or `--flows-root DIR` to auto-discover every fragment-containing subdirectory
  of `DIR` as its own flow (rescanned live, so adding/removing one is picked up without
  restarting). Every API route is now nested under `/api/flows/{flow_id}/...`, with a new
  `GET /api/flows` listing them. A ☰ menu appears in the graph's top-left corner to switch flows
  when there's more than one (persisted, and reflected in the URL as `?flow=<id>` for
  bookmarking/sharing); with just one flow — including today's single-directory invocation — the
  page is unchanged
* Removed the LR/TD layout toggle — the graph now always lays out left-to-right
* The inspector panel's "Config" section now renders each entry as a card (its key as a header,
  its value as a code block) instead of a table row, with an object value pretty-printed
* Consumer lag now has a Grafana-style refresh control in the graph's top-right corner: a "↻"
  button to refresh every visible edge's lag immediately, and a dropdown to set an auto-refresh
  interval (Off/5s/15s/30s/1m/5m, persisted, off by default, paused while the tab is hidden). The
  inspector's "Consumer lag" section also gets its own per-row "↻" to refresh just one edge
