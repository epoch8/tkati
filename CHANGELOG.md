# Changelog

One entry per jj change, keyed by its change identifier (stable across
`jj describe`/`jj squash`/rebases — use `jj log -r <change-id>` to look one
up). Newest first.

Each heading ends with the components the change actually touched, so `grep
'^###' CHANGELOG.md` shows what moved and where. A component is a workspace
package, or `repo` for changes to shared, root-level things (docs, CLAUDE.md, CI
beyond a package's own generated workflows). Because every package's
`pyproject.toml` is bumped on every release, a version bump alone does **not**
make a package a component of the change — list only packages whose code, tests,
config or docs changed.

## 0.5.0

### rmmvtyxs — Native Kafka consumer and producer [tkati-core, repo]

- The nodes were bottlenecked on JSON:
  - Producing Arrow as JSON built a Python dict per row (`to_pylist()`) and
    then ran orjson on each one: 7.4 s per 1M rows.
  - Consuming parsed a Python-assembled `BytesIO` as a single block, so on
    one core.
- `tkati_core._native` is a pyo3 extension. It owns the Kafka client (rdkafka,
  with librdkafka, OpenSSL and zstd statically linked) and does the heavy work
  with the GIL released. `KafkaConsumer`/`KafkaProducer` keep their Python
  interface as thin wrappers.
- Encoding: arrow-json per column, rayon over row ranges, one payload per row.
  It is 29–44x faster (0.24 s per 1M rows), and its output is byte-identical to
  the old path except for float formatting. Timestamps get a custom encoder so
  that their ISO strings keep `datetime.isoformat()`'s shape.
- Decoding stays on pyarrow's C++ reader, which benchmarked faster per core
  than arrow-json. What changed is its input: one contiguous NDJSON buffer,
  assembled natively while polling and exported zero-copy through the buffer
  protocol, with block sizes chosen so that every core gets a share. It is
  1.7–2.8x faster, with unchanged parsing semantics.
- `read_pylist`/`produce_pylist` stay on orjson, because building Python
  objects is GIL-bound.
- Broker-free parity tests (`tests/test_native_codec.py`) pin the old and new
  paths against each other. `benchmarks/bench_kafka_json.py` measures them.
- Packaging: tkati-core builds with maturin, as abi3 wheels for manylinux
  x86_64 and aarch64. The `test`/`publish` workflow templates gained a maturin
  variant: Rust caching, fmt/clippy/test, and a `maturin-action` wheel
  matrix. Dependent packages' test workflows cache tkati-core's Rust build.
  The variant lives inside the existing templates, not new template types, so
  that `publish-tkati-core.yml` keeps the filename PyPI trusted publishing is
  pinned to.
- The devcontainer gets `build-essential` and `perl` for the vendored C
  builds. CLAUDE.md covers the Rust workflow and adds `Cargo.toml` to the
  version bump checklist.

## 0.4.5

### nltnrvuo — Export LoopStats as Prometheus metrics [tkati-core, tkati-node-dedup]

- The perf breakdown was only a log line every 10s, which can't be graphed or
  alerted on. `tkati_core.metrics.LoopStatsCollector` exposes the same numbers
  as Prometheus counters:
  - `tkati_phase_seconds_total{node,phase}`
  - `tkati_wall_seconds_total{node}`
  - `tkati_rows_in_total` and `tkati_rows_out_total`
  - `tkati_iterations_total` and `tkati_starved_iterations_total`

  `start_metrics_server(MetricsSettings, stats)` serves them from a daemon
  thread.
- Everything is a monotonic counter, and rates and shares are left to PromQL.
  `tkati_wall_seconds_total` is the 100% denominator, so a phase's share is
  `rate(phase) / ignoring(phase) group_left rate(wall)`, which is exactly the
  log line's percentage. It is a separate metric rather than a `phase="total"`
  series, because a total inside the phase metric would be counted twice by
  `sum by (node)`.
- `LoopStats` keeps running totals that `reset()` folds each interval into, and
  `totals()` reads them at scrape time. A lock covers only the fold and the
  read, so a scrape can never catch a counter going backwards. The hot path
  (`record()`, `+=`) takes no lock and pays nothing for being exported.
- **`tkati-node-dedup` now listens on port 8000 by default.** This is visible
  to deployments. Opt out with `[metrics] enabled = false` or
  `METRICS__ENABLED=false`.
- New `tkati-core` dependency: `prometheus-client`.

### rzvtzzns — Promote read_pylist to the base Consumer [tkati-core]

- `read_pylist` was defined only on `KafkaConsumer`, so a caller holding the
  `Consumer` that `build_consumer` returns had to cast to call it. An app
  outside this repo calls it. It is now an abstract method on `Consumer`, next
  to `read_arrow`, with the same `stats` attribution. This matches `Producer`,
  which has always declared `produce_pylist`.
- The base docstring states how it differs from `read_arrow`: a message that
  fails to decode is skipped and logged instead of failing the batch.
- **Breaking** for any `Consumer` subclass outside this repo, which must now
  implement `read_pylist`. `KafkaConsumer`, the only implementation here, is
  unchanged.

### lkpyyqzq — Split the producer's time into serialize, enqueue and deliver; prefix core phases [tkati-core, tkati-node-dedup]

- Follows 0.4.4's consumer split. The dedup node's `produce` phase (~39%) was
  still one number covering three things with unrelated fixes: encoding rows
  (Python CPU), one `produce()` call per message into librdkafka (message
  count), and the blocking `flush` (broker acks). `Producer.produce_arrow`,
  `produce_pylist` and `flush` now take an optional `stats: LoopStats` and
  record **`producer/serialize`**, **`producer/enqueue`** and
  **`producer/deliver`**. `tkati_core.PRODUCER_PHASES` names them, for nodes to
  splice in.
- `KafkaProducer` used to encode each row and enqueue it in the same loop.
  It now encodes the whole batch first and then enqueues it, so the two can be
  timed separately. The messages produced are unchanged.
- `ClickhouseProducer` records its whole insert as `producer/deliver`, because
  `clickhouse_connect` encodes and sends in a single call. Its DLQ producer is
  not handed `stats`: the fallback already runs inside that block.
- `KafkaConsumer.read_pylist` now takes `stats` too, split like `read_arrow`.
- **Report format change:** every phase timed inside core is now prefixed with
  its component. `poll`/`parse` from 0.4.4 are renamed
  `consumer/poll`/`consumer/parse`, and `produce` is replaced (not wrapped) by
  the three `producer/` phases. Node-owned phases (`lookup`, `write`, `commit`)
  stay unprefixed. Anything that searches logs for `poll=` or `produce=` needs
  updating.

## 0.4.4

### wqmmyrsz — Split the consumer's read time into poll and parse [tkati-core, tkati-node-dedup]

- The dedup node's perf report showed `read` as its largest phase (~49%), but
  that single number covered both waiting on the Kafka broker and JSON-parsing
  the batch into Arrow — two things with unrelated fixes. `Consumer.read_arrow`
  now takes an optional `stats: LoopStats` and attributes its own time to
  **`poll`** and **`parse`** separately, so the next optimization has something
  to aim at.
- `tkati_core.CONSUMER_PHASES` is the ordered pair of names, exported so nodes
  splice it into their phase tuple instead of restating it — a rename in core
  would otherwise leave a node's column silently reading `0.00s`.
- `read` is **replaced** by `poll`/`parse` in `tkati-node-dedup`'s report, not
  nested inside them. An umbrella phase would double-count that time and break
  the property that phase percentages fall short of 100%, with the shortfall
  being genuinely unaccounted work.
- Note when reading `poll`: it is not pure broker wait. Handing each message
  from librdkafka to Python costs ~0.8 us even against a fully pre-buffered
  topic, so a batch of 2000 carries ~1.6ms of floor. Measured live, that was
  about a tenth of `poll`.
- The consumer's per-batch `logger.info` lines are now `logger.debug`. At
  production throughput they were tens of lines a second, which buried the
  perf report; the cost itself was only ~0.1% of wall clock. Row-count
  mismatches and parse failures still log at `warning`/`error`. **This changes
  `tkati-node-el`'s log output too**, which is otherwise untouched.

## 0.4.3

### rsmnyupm — Speed up the dedup store's RocksDB path [tkati-core, tkati-node-dedup]

- `BucketedDedupStore` ran entirely on RocksDB's defaults: **no bloom filter at
  all** (`filter_policy` is nullptr unless you configure one), an 8MB block
  cache, no memtable bloom, and Snappy compression. Since duplicates are rare in
  practice, nearly every lookup is a miss — the exact case a bloom filter exists
  to make free. Measured on a 5M-key bucket at a 2% hit rate, all in
  `benchmarks/bench_store.py`: **4.32 → 0.80 µs/key, a 5.4x speedup**, with
  data-block reads down ~40x. Writes cost ~10% more (1.63 → 1.80 µs/key).
- **`BlockBasedOptions.set_bloom_filter()` is broken in rocksdict 0.3.29** (the
  current release) and the store deliberately does not use it. It writes the
  filter into the SSTs — the on-disk size grows by exactly bits x keys — but the
  read path never consults it: `rocksdb.bloom.filter.useful` stays at 0 and the
  data-block read count is identical with the filter on and off. Verified for
  `raw_mode` on and off, block-based and full filters, format versions 5 and 6,
  and `get`/batched multi-get/`key_may_exist` alike. The working path is
  `Options.optimize_for_point_lookup()`, which reaches RocksDB's own helper;
  note that calling `set_block_based_table_factory()` afterwards silently undoes
  it. The helper fixes bits-per-key at 10, so there is no knob for it.
- Memtable whole-key bloom at ratio 0.02. Only measurable against a *warm*
  memtable (3.23 → 0.85 µs/key); higher ratios are worse, not better, because a
  larger bloom probes with worse cache locality.
- Compression defaults to `none`: Snappy measured 2x slower on reads (2.84 vs
  1.37 µs/key) to save ~30% disk on keys that are high-entropy and barely
  compress, in a store that is deleted within the hour.
- WAL disabled for dedup writes, worth ~5% on the write path. The dedup store is
  now explicitly not crash-durable: a hard kill loses up to one write buffer of
  state, which forwards some duplicates but can never drop an event. Kafka's
  committed offset, not RocksDB, is the durability boundary. README updated;
  `disable_wal = false` restores the old behavior.
- Two changes that look obviously right for this workload measured **worse** and
  are documented in `RocksDBSettings` so they don't get "fixed": a 256MB write
  buffer (25% worse writes, 20% worse reads than 64MB), and disabling
  auto-compaction (bought nothing on writes, since compaction runs on background
  threads, while costing 2.2x on reads as L0 files accumulated).
- New `[dedup.rocksdb]` settings block, all of it performance-only —
  `test_tuning_variants_do_not_change_semantics` parametrizes the store across
  every knob and asserts the answers never change.
- The node now logs where its wall clock went every 10 seconds — `read`,
  `lookup`, `produce`, `write` and `commit` as seconds and percent of the
  interval — plus a `starved` count for iterations that were waiting on the
  broker rather than CPU-bound. Percentages are of the interval rather than of
  each other, so they don't sum to 100 and unaccounted time stays visible. The
  former per-batch `INFO` line is now `DEBUG`; its counts are in the interval
  line. The store itself carries no instrumentation: `bench_store.py` brackets
  the calls it wants to time, so nothing measures the production hot path.
- That instrumentation lives in `tkati-core` as `LoopStats`, not in this node:
  every node's loop has the same shape, and `tkati-node-el`'s differs only in
  having no lookup or write phase. Phase names, log prefix and cadence are
  constructor arguments; the dedup node supplies its own five phases. As a
  consequence of generalizing, the in/out delta is now labelled `dropped`
  rather than `deduped`, since not every node that filters is deduplicating.
- Fixed: a bucket that failed to open at startup was never in `_dbs`, so
  `cleanup_expired` could not reach it and it sat on disk until the next
  restart. It now sweeps the filesystem too.
- `encode_keys` casts to `pa.binary()` instead of `.encode()`-ing each row (~25%
  off that step). The cast stays two-step because pyarrow has no direct
  int64->binary cast and the dedup field is routinely an integer column.

## 0.4.2

### rwoxuqpw — Make the dashboard follow the light/dark color scheme [tkati-dashboard]

- Every color in `packages/tkati-dashboard/src/tkati_dashboard/static/index.html` is now a CSS
  custom property defined once in the `<style>` block as a `light-dark(<light>, <dark>)` pair,
  with `color-scheme: light dark` on `:root` (plus the matching `<meta>`). Light values are the
  previous hardcoded hexes, so light mode is unchanged. Inline styles in the React code reference
  the tokens as `"var(--…)"` strings — including `GROUP_COLORS`/`FALLBACK_COLORS` and
  `SELECTED_EDGE_COLOR` — so a scheme change repaints in CSS alone: no `matchMedia` listener, no
  re-render, and `layout()`'s memoized dagre run isn't touched.
- React Flow v11 has no dark mode of its own (`colorMode` is v12+), so its stylesheet's
  hardcoded colors (edge paths, handles, controls, minimap, attribution) are overridden in the
  same `<style>`, which loads after the esm.sh `<link>`. The minimap node fill, minimap mask, and
  background dots are SVG presentation attributes in v11, so they're themed with CSS rules rather
  than props: `MiniMap` switched from `nodeColor` to `nodeClassName` (`minimap-node--<group>`,
  with `group` added to each node's `data`).
- Arrowheads: `<ReactFlow defaultMarkerColor="var(--edge)">` for plain edges. v11 applies marker
  colors through the polyline's inline `style`, where `var()` resolves. The highlighted edge's
  marker uses `SELECTED_EDGE_COLOR` (`var(--accent)`) the same way.
- `light-dark()` requires Chrome 123, Firefox 120, or Safari 17.5 (all 2024).

## 0.4.1

### ulyqzzps — Break consumer lag down per partition in the dashboard inspector [tkati-dashboard]

- The inspector panel's "Consumer lag" section (`ConsumerLagSection` in
  `packages/tkati-dashboard/src/tkati_dashboard/static/index.html`) renders each consuming
  edge as its own block: the existing summary row (`← other-node (group_id)`, the group's total
  lag, a per-row "↻") followed by a new `PartitionLagTable` listing `partition`/`lag` for every
  partition, sorted by partition id. An aggregate alone can't tell a backlog spread evenly over
  a topic's partitions from one stuck partition holding all of it.
- No backend change: `lag.fetch_consumer_lag` has always returned `partitions[]` (with
  `partition`, `committed_offset`, `high_watermark`, `lag`) next to `total_lag`, and
  `GET /api/flows/{flow_id}/nodes/{id}/consumer-lag` has always passed it through — the frontend's
  `fetchLag` was dropping everything but `total_lag`, and now keeps the array in `lagByEdge`.
  So the breakdown costs no extra broker round trips and rides the existing refresh paths
  (initial load, auto-refresh interval, per-row and global "↻") unchanged.
- The table renders only for a resolved lag, so a loading or errored edge — or a topic with no
  partitions — shows just its summary row as before. It scrolls within a `maxHeight: 160`
  container, matching `TopicStats`, so a high-partition-count topic doesn't push the panel's
  later sections off screen.
- The graph's own labels (`edgeLabel`) stay aggregate-only deliberately: their line count feeds
  the canvas text measurement that sizes nodes for dagre, so per-partition lines there would
  relayout the graph on every refresh tick.

## 0.4.0

### mtxpxzkt — Add tkati-dashboard: multi-flow dataflow graph viewer [tkati-dashboard, repo]

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

### xwxxmoms — Preserve timestamp[ms] through Kafka JSON round trip [tkati-core, tkati-node-dedup, tkati-node-el]

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

### twqstomm — Add tkati-node-dedup: Kafka-to-Kafka streaming dedup node [tkati-node-dedup]

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
