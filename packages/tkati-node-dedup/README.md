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
```

Output and DLQ follow the same `OutputSettings` shape as `tkati-node-el`
(`"kafka"` or `"clickhouse"`) — see that package's README for the full
connection/table config shape.

## Dedup store performance

Every 10 seconds the node logs where its wall clock went, using
`LoopStats` from `tkati-core`:

```
dedup perf over 10s: 157000 rows in, 153880 out (3120 dropped), 157 iterations (0 input-starved)
dedup perf: poll=4.43s (44%) parse=0.48s (5%) lookup=0.52s (5%) produce=3.96s (39%) write=0.21s (2%) commit=0.38s (4%)
```

`dropped` is the rows this node deduplicated away.

* `poll` — fetching message batches from the broker. Mostly broker round trips,
  but it also includes librdkafka handing each message to Python, which has a
  floor of roughly 0.8 us/message no matter how fast the broker is
* `parse` — JSON-decoding those payloads into an Arrow table, and casting to
  the internal schema
* `lookup` — encoding keys, resolving in-batch duplicates, querying the store
* `produce` — serializing and producing, including the blocking `flush`
* `write` — marking the surviving keys seen
* `commit` — the synchronous offset commit (and bucket cleanup, which is ~0
  except once an hour when a bucket is destroyed)

`poll` and `parse` come from `tkati-core`'s consumer rather than from this
node, which splices them in from `CONSUMER_PHASES`. They are split apart
because their fixes are unrelated: a large `poll` points at batch sizing,
broker latency or an under-fed topic, while a large `parse` points at the JSON
decode and is what a faster wire format would address.

Percentages are of the interval, not of each other, so they **do not sum to
100** — the remainder is time in none of the named phases.

`input-starved` counts iterations where the node drained the topic and waited
out the batch timeout. Those iterations were not CPU-bound, and because `poll`
blocks for the whole wait, a mostly-starved interval will show `poll` at close
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

**Delivery: at-least-once.** Offsets are committed only after (1) the
filtered batch is produced and confirmed delivered (`produce_arrow` followed
by a blocking `flush`), and (2) the surviving keys are recorded in the current
RocksDB bucket. If the process crashes between steps, the same input batch is
re-read at restart; because the keys from a completed produce are already
marked seen, re-processing that batch is a no-op (or reproduces only the
genuinely-new subset) rather than losing data.

**The dedup store is not crash-durable, by design.** With `disable_wal`
(the default) writes go to a volatile memtable, so a hard kill can lose up to
one write buffer's worth of dedup state — those keys stop being recognized as
seen, and later duplicates of them are forwarded. It can never cause an event
to be dropped, which is the tradeoff this node makes everywhere: Kafka is the
source of truth and the committed offset, not RocksDB, is the durability
boundary. A *graceful* shutdown flushes and loses nothing. Set
`dedup.rocksdb.disable_wal = false` to trade throughput for crash durability.

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
