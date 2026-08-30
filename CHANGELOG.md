# Changelog

One entry per jj change, keyed by its change identifier (stable across
`jj describe`/`jj squash`/rebases — use `jj log -r <change-id>` to look one
up). Newest first.

## WIP 0.4.0

### yywonqyq — tkati-dashboard: highlight a selected node's edges in the graph

- Every edge into or out of the selected node now turns the same blue as the node's own
  selection glow (`SELECTED_EDGE_COLOR`, matching `rgba(29, 78, 216, ...)`) and gets a higher
  `zIndex` so it draws above any edge it happens to cross — added to `flowEdges`'s existing
  per-edge `style`/`markerEnd` (already forwarded to `BaseEdge` by `LabeledEdge`, so no change
  needed there), computed from `selectedNodeId` alongside the edges that were already
  recomputed on selection.

### xqtxwrls — tkati-dashboard: keep consumer lag fresh with a Grafana-style refresh control

- Kafka has no push API for consumer lag (it's always re-derived from committed offset vs. high
  watermark), so a new `RefreshControl` — a ☰-corner `Panel` in the canvas's top-right, shown
  whenever the graph has at least one lag-carrying edge (`relevantLagEdges()`) — lets the viewer
  decide how (and whether) to keep re-querying it: a "↻" button refreshes every visible edge's
  lag immediately, and a `REFRESH_INTERVAL_OPTIONS` dropdown (Off/5s/15s/30s/1m/5m, `usePersistedState`
  under `tkati-dashboard-lag-refresh-interval`, defaulting to Off) sets an auto-refresh interval.
  The interval is skipped while `document.hidden`, so a background tab doesn't keep hitting the
  broker for nothing.
- `App`'s lag-fetching logic is consolidated into one `fetchLag(flowId, edge)` callback shared by
  the initial load, the auto-refresh timer, and both manual refresh affordances, so they can't
  disagree about what "refresh the lag" means. A previously-`"ready"` value now stays on screen
  (only a new `fetching` flag flips) while a refresh is in flight, instead of blanking to
  `"loading…"` every cycle — the initial load still shows `"loading…"` since there's nothing to
  preserve yet.
- The inspector's "Consumer lag" section (no longer a plain `KVTable`) gets its own per-row "↻",
  refreshing just that one edge on demand — a global refresh (or waiting for the next tick)
  would otherwise touch every edge, not just the one being looked at.

### sqkmzpro — tkati-dashboard: render node config as cards instead of a table

- The inspector panel's "Config" section (a node's own `config`, not `TopicStats`'s topic-level
  config table) now renders each entry as its own card — the key as a small header band, the
  value as a `<pre><code>` block below it — instead of a `KVTable` row. New `ConfigCards`
  component; new `formatCodeValue()` pretty-prints an object value (`JSON.stringify(v, null, 2)`,
  matching how `EventSnapshot` already renders a message body) rather than `formatCell`'s
  single-line `JSON.stringify`, since a config value is at least as likely to be a nested object
  as a scalar and a narrow table column doesn't give it room to read well.

### kmoslqtw — tkati-dashboard: observe multiple flows from one dashboard instance

- The CLI's single positional `dataflow_dir` is now `nargs="+"` — pass more than one directory to
  observe several dataflows from one server instead of running one instance per flow. A flow's id
  is its directory's own basename, same identity `Dataflow.name` already had; two directories
  sharing a basename is now a startup error asking you to rename one of them.
- New `--flows-root DIR` (repeatable): every immediate subdirectory of `DIR` containing dataflow
  fragments becomes its own flow, discovered fresh on every `/api/flows` request rather than once
  at startup — adding or removing a flow directory there is picked up without restarting, the same
  "always read live" philosophy `load_dataflow` already applies to one flow's fragments. New
  `tkati_dashboard.flows` module (`discover_flows_root`/`make_flow_lister`) implements this,
  combined with any explicit directories into one `list_flows()` resolver passed into `create_app`.
- `create_app` now takes that `list_flows` callable instead of a single `Path`. Every route is
  nested under a `flow_id` path segment — `/api/graph` → `GET /api/flows/{flow_id}/graph`, and
  likewise for the three `/api/nodes/{id}/...` routes — plus a new `GET /api/flows` listing every
  currently-resolved flow (`{id, name}`, sorted by name). An unknown `flow_id` (or a
  `--flows-root` directory removed since the process started) 404s instead of erroring.
- The frontend now fetches `/api/flows` first, then the selected flow's graph from
  `/api/flows/{id}/graph`; every per-node fetch (snapshot/topic-stats/consumer-lag) threads the
  selected flow id through the same prop path (`InspectorPanel` → `NodeDetails` →
  `EventSnapshot`/`TopicStats`). Switching flows drops the previous flow's selected node and
  fetched lag, since neither's ids necessarily mean anything against a different flow's graph.
- New `FlowMenu`: a ☰ button in the canvas's top-left corner naming the current flow, expanding
  into a dropdown on click and collapsing again on a pick or an outside click — so it costs almost
  no canvas space once a flow is chosen. Rendered only when there's more than one flow; with
  exactly one (today's common case, and what a single-directory invocation still gives you) the
  page looks and behaves exactly as before. The selected flow is also persisted (`localStorage`,
  via the existing `usePersistedState`) and reflected into the URL as `?flow=<id>` so a specific
  flow's view is bookmarkable/shareable, with the URL param taking precedence over the persisted
  choice on load.
- Removed the LR/TD layout direction toggle (`LayoutDirectionToggle`, added back in `ukyykwzq`) —
  the graph now always lays out left-to-right; `layout()`/`StackedNode` no longer take/branch on a
  `direction`, since dagre's `rankdir` and each handle's `sourcePosition`/`targetPosition` are now
  fixed to the LR case that was already the only one anyone used.

### ntsxvmtt — tkati-dashboard: fold consumer-edge info into the consuming node as stacked rows

- An incoming edge with a consumer group (i.e. something to show beyond its bare `kind`) no
  longer floats its `group`/`lag` as a label on the edge line — it gets its own row stacked
  below the consuming node's header instead (`isRowEdge`/`layout()` in `static/index.html`), one
  row per such input. A fan-in node consuming two topics (e.g. a sessionizer joining
  deduped-clicks and deduped-purchases) now shows two rows, each with its own `Handle` so that
  edge's arrow visibly lands on *its* row, not the header — `DEFAULT_TARGET_HANDLE` is the
  fallback target for edges with nothing extra to show, which keep today's plain one-word `kind`
  label.
- New custom ReactFlow node type (`StackedNode`, registered via `NODE_TYPES`) renders the
  header + rows and owns the selected-node glow (moved from a top-level `style` override into
  `data.isSelected`, now that individual node markup lives in this component rather than a
  single styled `<div>`).
- `layout()` now groups edges by target before measuring, since a node's box height is
  `header height + Σ(row heights)` and width is the widest of those — dagre still only ever
  sees one final box per node, unchanged otherwise. `edgeLabel()` reserves the `lag` line as
  soon as there's a consumer group (a "…" placeholder before the fetch resolves) instead of
  growing by a line once it does, so a row/edge label's box doesn't change shape underneath
  dagre's already-computed layout moments after load.
- `<MiniMap>` now takes an explicit `nodeColor` reading `data.colors` — it previously read the
  now-removed top-level `style.background`.
- Verified with the same headless dagre harness used for the original size-aware layout change:
  re-derived real `/api/graph` output for both bundled examples, confirming `sessionize` (two
  consumer inputs) grows two rows, single-input nodes grow one, topic nodes stay row-less, and
  zero box overlaps in both LR and TD across all 12 analytics-pipeline nodes, with every
  row-worthy edge resolving to the exact row `layout()` produced for it.
- Fixed the header's and each row's own square-cornered border being abruptly clipped by the
  outer wrapper's rounded corners (relying purely on `overflow: hidden` to cut a plain
  rectangle into a rounded one looks chopped, not curved) — the header now carries the top
  radius (or all four corners if it has no rows) and only the last row carries the bottom
  radius, so each section's own border follows the curve instead of being sheared off by it.
- Fixed a row-targeting edge's arrow landing in empty space past the node instead of on its
  row. First pass: ReactFlow measures a node's Handle positions once, and different nodes here
  have different handle counts (one per row plus a default) — a documented "dynamic handles"
  gotcha where it doesn't notice that set changing on its own. Added `useUpdateNodeInternals`,
  called for every node whenever `nodes` changes, and wrapped the app in `ReactFlowProvider`
  (required for that hook, and not otherwise provided to a component sitting above
  `<ReactFlow>` rather than inside it). That alone wasn't the whole fix: a row's own Handle is
  rendered *inside* that row's own `position: relative` div, so `handleStyle()`'s `top` needs
  to be relative to just that row's height — it was computing an offset relative to the whole
  node instead (header height + every preceding row), landing well past the row's actual
  bounds once the browser resolved it against the wrong parent.
- A row's color is now a deeper shade of its node's own `GROUP_COLORS` (a new
  `rowBackground`/`rowText` pair per group, plus a `FALLBACK_COLORS` object for an unrecognized
  group) instead of a fixed indigo — so a row reads as part of its node's own color, not an
  unrelated accent.

### urwupwnw — tkati-dashboard: fix nodes vanishing on resize and garbling on selection

- `layout()` (canvas text measurement + a full dagre run for every node) was recomputed on
  every render of `App`, including the dozens of `mousemove`-driven re-renders per second while
  dragging the inspector panel's resize handle — that churn raced ReactFlow's own resize-driven
  viewport recalculation and made every node disappear mid-drag.
- Wrapped the node/edge computation in `React.useMemo`, keyed on `[graph, direction,
  selectedNodeId]` / `[graph, lagByEdge]` respectively, so panel-resize state (`panelWidth`,
  `resizing`) no longer triggers a relayout at all — moved above `App`'s early returns to keep
  hook call order unconditional, per the Rules of Hooks.
- Fixed a node's text overflowing past its bottom border when selected (visible on a node with
  a label line close to the box's measured width, e.g. a multi-broker comma-separated
  `connection.broker`). The selection highlight changed `border` from `1px` to `2px solid`;
  since nodes use `boxSizing: border-box` with a fixed width/height computed by
  `measureNodeBox()` assuming a constant 1px border, the wider border shrank the content area
  just enough to wrap an extra line, which then overflowed the (still 1-line-shorter) box
  height. Selection now only changes the border's *color*, leaving its width — and thus the
  content area — unchanged; `boxShadow`, which never affects layout, carries the emphasis.

### ukyykwzq — tkati-dashboard: LR/TD toggle, selection highlight, multi-line edges, lag in panel

- Added an LR/TD layout toggle (`LayoutDirectionToggle`, a `Panel` in the graph's top-left
  corner), persisted to `localStorage` via a new `usePersistedState` hook — LR (left-to-right)
  is now the default, dagre's `rankdir` and each node's `sourcePosition`/`targetPosition` switch
  with it.
- The selected node's box now gets a visible blue border/glow in the graph, not just a filled-in
  inspector panel — there was previously no indication in the graph itself of which node the
  panel was showing.
- Edge labels are now multi-line (`kind`, then `group: …`, then `lag: …`, one per line) instead
  of a single `·`-joined line. ReactFlow's default label is SVG `<text>` and can't render `\n`,
  so this required a custom edge type (`LabeledEdge`, via `BaseEdge`/`EdgeLabelRenderer`/
  `getBezierPath`) rendering the label as an HTML div instead.
- The inspector panel has a new "Consumer lag" section: every stream edge touching the selected
  node (either direction) that names a consumer group, with its live lag — reusing the lag
  already fetched for the graph's edge labels rather than fetching it again.

### xvxosztm — tkati-dashboard: size-aware graph layout via dagre

- Replaced the hand-rolled BFS rank/column layout with [dagre](https://github.com/dagrejs/dagre)
  (`@dagrejs/dagre`, loaded from esm.sh like React/ReactFlow), fed each node's real box size
  instead of a flat 240×160 grid. A node's size depends on its label — `kafka-topic` nodes show
  `broker`/`topic`, and a real broker string (AWS MSK, Confluent Cloud, ClickHouse Cloud) can run
  50-70+ characters — so the old flat grid let long labels visually overlap their neighbors.
  `measureNodeBox()` gets each label's exact pixel size via an offscreen `<canvas>` +
  `ctx.measureText()`, with the node's `style.width`/`style.height` set to match exactly (plus
  `boxSizing: border-box`), so the rendered DOM node is exactly the size dagre assumed when
  placing everything else around it.
- Beyond fixing overlap, dagre also orders nodes within a rank to reduce edge crossings and
  handles rank assignment robustly (including an accidental cycle, e.g. a DLQ/reprocessing
  edge) — both beyond what the from-scratch BFS attempted.
- Verified with a headless Node.js harness (mirroring `layout()`/`measureNodeBox()` exactly,
  swapping in an approximate text-width function since there's no DOM canvas outside a browser)
  against the real `analytics-pipeline`/`simple-pipeline` example graphs and a synthetic graph
  with a 70-character AWS-MSK-style broker string: zero overlapping boxes in all three.

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
