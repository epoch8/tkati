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
```

Output and DLQ follow the same `OutputSettings` shape as `tkati-node-el`
(`"kafka"` or `"clickhouse"`) — see that package's README for the full
connection/table config shape.

## Delivery & dedup guarantees

**Delivery: at-least-once.** Offsets are committed only after (1) the
filtered batch is produced and confirmed delivered (`produce_arrow` followed
by a blocking `flush`), and (2) the surviving keys are durably written to the
current RocksDB bucket. If the process crashes between steps, the same input
batch is re-read at restart; because the keys from a completed produce are
already marked seen, re-processing that batch is a no-op (or reproduces only
the genuinely-new subset) rather than losing data.

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
