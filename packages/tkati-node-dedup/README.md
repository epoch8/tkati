# tkati-node-dedup — streaming deduplication node

Reads batches from a Kafka input topic, drops events that are duplicates of an
event seen on the same `field` within a rolling processing-time window, and
writes the deduplicated batch to a configurable output. Duplicate state is
tracked in an embedded, on-disk RocksDB store local to this process — no
external dedup service is required.

## Configuration

Settings are loaded from a TOML file. Set the `SETTINGS_FILE` environment
variable to point to it (defaults to `settings.toml`).

```toml
[input]
type = "kafka"

[input.connection]
broker = "redpanda:29092"

[input.topic]
name = "raw_event"

[input.topic.schema]
uid  = "string"
time = "timestamp[ms]"
# … other columns

[input.consumer]
group_id          = "node-dedup-group"
batch_size        = 1000
batch_timeout_sec = 10
auto_offset_reset = "latest"

[output]
type = "kafka"

[output.connection]
broker = "redpanda:29092"

[output.topic]
name = "raw_event_deduped"

[dedup]
field        = "uid"     # column in input.topic.schema to dedup by
window_hours = 3         # rolling dedup window
bucket_hours = 1         # on-disk bucket granularity (effective window is
                          # window_hours .. window_hours + bucket_hours)
store_dir    = "/var/lib/tkati-node-dedup/store"

# Optional. Performance tuning for the embedded store; the defaults below are
# the ones shipped, and were chosen by A/B measurement rather than reasoning
# (see "Dedup store performance").
[dedup.rocksdb]
point_lookup_optimized   = true    # RocksDB bloom filter + block cache
memtable_bloom_ratio     = 0.02    # 0 disables
block_cache_mb           = 128
write_buffer_mb          = 64
compression              = "none"  # "none" | "lz4" | "zstd" | "snappy"
disable_wal              = true
disable_auto_compactions = false   # benchmarking only; see below
enable_statistics        = false   # costs ~5-10%; diagnosis only

# Optional. Prometheus endpoint, ON by default; these are the defaults.
[metrics]
enabled = true
port    = 8000
addr    = "0.0.0.0"
```

The node serves Prometheus metrics at `:8000/metrics` unless told not to. To
turn it off, set `[metrics] enabled = false` or the env var
`METRICS__ENABLED=false`; `METRICS__PORT` moves it. The metrics are the same
numbers as the perf log line described below; see `tkati-core`'s README for
the metric names and the PromQL that reproduces the log line's percentages.
On top of those, `tkati_node_dedup_dropped_rows_total` counts the rows this
node deduplicated away (the log line's `dropped`), once their batch is
committed.

Output and DLQ follow the same `OutputSettings` shape as `tkati-node-el`
(`"kafka"` or `"clickhouse"`) — see that package's README for the full
connection/table config shape.

## Dedup store performance

Every 10 seconds the node logs where its wall clock went, using
`LoopStats` from `tkati-core`:

```
perf over 10s: 157000 rows in, 153880 out (3120 dropped), 157 iterations (0 input-starved)
perf loop: wait/input=0.21s (2%) lookup=0.52s (5%) producer/serialize=2.10s (21%) producer/enqueue=0.35s (4%) producer/deliver=0.00s (0%) wait/in-flight=1.12s (11%) commit=0.38s (4%) write=0.21s (2%)
perf read: consumer/poll=4.43s (44%) consumer/parse=0.48s (5%) wait/loop=4.90s (49%)
```

`dropped` is the rows this node deduplicated away.

`perf loop:` is the node's loop, in the order its phases happen for a batch.
`perf read:` is its read-ahead thread, which reads the next batches while the
loop works on this one. The two lines run at the same time, so add up
percentages within a line, never across them.

* `consumer/poll`: fetching message batches from the broker. Mostly broker
  round trips, but it also includes librdkafka handing each message to Python,
  which costs at least about 0.8 us per message however fast the broker is.
* `consumer/parse`: JSON-decoding those payloads into an Arrow table and
  casting to the internal schema.
* `lookup`: encoding keys, resolving in-batch duplicates, querying the store.
* `producer/serialize`: converting the surviving rows to JSON (or to Arrow IPC
  for `arrow-batch`).
* `producer/enqueue`: handing each encoded message to librdkafka.
* `producer/deliver`: a ClickHouse output records its whole insert here. A
  Kafka output doesn't wait for acks per batch, so it reads 0; see
  `wait/in-flight`.
* `write`: marking the surviving keys seen, once their batch is committed.
* `commit`: the offset commit, plus bucket cleanup, which is ~0 except once
  an hour when a bucket is destroyed.
* `wait/input`: time the loop waited for the next batch from its read-ahead
  thread. High means the node is input-bound.
* `wait/in-flight`: time `done()` waited because `[pipeline] max_in_flight`
  batches were still undelivered. High means the node is output-bound.
* `wait/loop` (read line): time the read-ahead thread waited for the loop to
  take what it had read. High, with low `wait/input` and `wait/in-flight`,
  means the loop's own work (lookup, serialize, write) is the bottleneck.

The `consumer/` and `producer/` phases are timed by `tkati-core` rather than by
this node, which splices the producer's in from `PRODUCER_PHASES`. The
consumer's are on the read line, which `tkati-core` adds itself.
They are split apart because their fixes are unrelated:

* A large `consumer/poll` points at batch sizing, broker latency or an
  under-fed topic.
* A large `consumer/parse` points at the JSON decode, which a faster wire
  format would address.
* A large `producer/serialize` is Python-side encoding cost.
* A large `producer/enqueue` means too many small messages.
* A large `producer/deliver` points at the broker, or at the output's acks and
  `linger.ms` settings.

Percentages are of the interval, not of each other, so they **do not sum to
100** — the remainder of a line is time in none of its named phases.

`input-starved` counts iterations where the node drained the topic and waited
out the batch timeout. Those iterations were not CPU-bound, and because
`consumer/poll` blocks for the whole wait, a mostly-starved interval will show
`consumer/poll` at close
to 100% and tells you nothing about whether the node can keep up.

`benchmarks/bench_store.py` A/B tests the store in isolation. It populates in
one process and measures in a fresh one, because a store that has just been
written has everything in its memtable and every table-level option looks
like it does nothing.

**Turning RocksDB's bloom filter on is the single largest win**, because it
was off: RocksDB has no filter policy by default, so every negative lookup —
and with rare duplicates nearly every lookup is negative — read data blocks
out of the SSTs. Enabling it cut data-block reads by ~40x.

**Caveat, and the reason the code looks the way it does:** the obvious API,
`BlockBasedOptions.set_bloom_filter()`, **does not work in rocksdict 0.3.29**
(the current release). It writes the filter into the SST files but the read
path never consults it — `rocksdb.bloom.filter.useful` stays at 0 and the
data-block read count is identical with the filter on and off. Only the
all-in-one `Options.optimize_for_point_lookup()` helper works, and calling
`set_block_based_table_factory()` after it silently undoes it. Recheck this
if rocksdict is ever upgraded.

Two changes that look obviously right for this workload measured *worse* and
are deliberately not enabled — don't "fix" them without re-running the
benchmark:

* **A larger write buffer.** 256MB measured 25% worse on writes and 20% worse
  on reads than 64MB.
* **Disabling compaction.** Each bucket is deleted within the hour, so its
  compaction looks like pure waste — but it bought nothing on writes (2.41 vs
  2.43 µs/key; compaction runs on background threads and never contended with
  the write path) while costing 2.2x on reads as L0 files accumulated.

## Delivery & dedup guarantees

**Delivery: at-least-once.** The node runs on `tkati-core`'s
`PipelinedNode`: it sends a batch and moves on to the next one while the
first is still being delivered. For each batch it (1) produces the filtered
batch, (2) once that batch and every earlier one is delivered, commits the
input offsets, and only then (3) records the surviving keys in the current
RocksDB bucket. Until then the keys are held in memory as *pending*, and the
next batches are checked against them as well as the store, so a key sent in
one batch is dropped from the next even before the first is delivered.

A key is never marked seen before its row is delivered, which is what
guarantees no event is lost. A crash before (2) loses the pending keys along
with the uncommitted offsets, so the batches are re-read and produced again: a
duplicate at worst. A crash between (2) and (3) leaves those keys unmarked, so
a later duplicate of one of them is forwarded, also a duplicate at worst. That
window is one memtable write wide, much narrower than the loss the WAL-off
store already accepts (below). Up to `[pipeline] max_in_flight` batches
(default 4) may wait for delivery at once.

**The dedup store is not crash-durable, by design.** With `disable_wal`
(the default) writes go to a volatile memtable, so a hard kill can lose up to
one write buffer's worth of dedup state — those keys stop being recognized as
seen, and later duplicates of them are forwarded. It can never cause an event
to be dropped, which is the tradeoff this node makes everywhere: Kafka is the
source of truth and the committed offset, not RocksDB, is the durability
boundary. A *graceful* shutdown flushes and loses nothing. SIGTERM, which is
what a pod gets on termination, is graceful, and so is SIGINT: the node
finishes the batch in hand, commits it, closes the store and exits. A second
signal forces an exit. Set `dedup.rocksdb.disable_wal = false` to trade
throughput for crash durability.

**On any internal dedup-store failure — a bucket won't open, a lookup errors,
a disk I/O error — the node treats the event as NOT a duplicate and forwards
it.** This node will occasionally forward a duplicate it should have caught,
but will never silently drop a real event because of dedup-store trouble.

**The window is approximate, not exact.** Because state is bucketed in
`bucket_hours` increments (default 1h) rather than a true sliding window, the
effective dedup window is between `window_hours` and
`window_hours + bucket_hours`. Stale buckets are deleted from disk
automatically once they fall outside the window — checked once per iteration,
so state never grows unbounded.

**Bucketing is by processing time**, not any timestamp field in the event
payload — an event's bucket is when this node handles it, not when it
happened upstream.

## IMPORTANT: dedup state is local to this process

The RocksDB store lives on local disk at `store_dir` and is **not shared**
between instances. Running multiple concurrent instances of this node against
the same input topic (e.g. multiple consumers in the same consumer group, or
multiple replicas) will **not** dedup correctly across instances unless the
input is partitioned such that all events sharing a `field` value are always
routed to the *same* instance (e.g. Kafka partitioning keyed on `field`, one
node instance per partition or partition subset it exclusively owns). Running
this node with more parallelism than that will let duplicates leak through
across instance boundaries. This is a direct consequence of choosing an
embedded local-file store instead of a shared external one — evaluate this
tradeoff before scaling this node horizontally.
