# 0.5.0

* New Prometheus counter `tkati_node_dedup_dropped_rows_total`: rows dropped
  as duplicates. The same number was only derivable before as
  `tkati_rows_in_total - tkati_rows_out_total`

# 0.4.5

* The perf report breaks the old `produce` phase into `producer/serialize`,
  `producer/enqueue` and `producer/deliver`, and renames `poll`/`parse` to
  `consumer/poll`/`consumer/parse`. `produce` is gone rather than kept as an
  umbrella. See the README
* **Now listens on port 8000 by default**, serving the perf report's numbers
  as Prometheus metrics at `/metrics`. Disable with `[metrics] enabled = false`
  or `METRICS__ENABLED=false`

# 0.4.4

* The perf report now breaks the old `read` phase into `poll` and `parse`,
  separating time spent waiting on the broker from time spent JSON-decoding
  into Arrow. `read` is gone rather than kept as an umbrella, so the
  percentages still don't double-count. See the README

# 0.4.3

* Tuned the embedded RocksDB store for its actual workload — lookups that
  almost always miss, because duplicates are rare. The store previously ran on
  RocksDB's defaults, which include **no bloom filter at all**. Measured on a
  5M-key bucket at a 2% hit rate: **4.32 → 0.80 µs/key on lookups, a 5.4x
  speedup**; writes cost ~10% more
* New optional `[dedup.rocksdb]` settings block (bloom filter, memtable bloom,
  block cache, write buffer, compression, WAL). Every knob is
  performance-only and tested not to change any answer
* **The dedup store is no longer crash-durable.** The WAL is disabled by
  default, so a hard kill loses up to one write buffer of dedup state — that
  forwards some duplicates, and can never drop an event. A graceful shutdown
  loses nothing. Set `dedup.rocksdb.disable_wal = false` to revert
* Note for maintainers: `BlockBasedOptions.set_bloom_filter()` does not work
  in rocksdict 0.3.29 — it writes the filter but the read path ignores it.
  The store uses `Options.optimize_for_point_lookup()` instead. See the
  README and `BucketedDedupStore._build_options`
* The node now logs where its wall clock went every 10 seconds — `read`,
  `lookup`, `produce`, `write`, `commit` as seconds and percent of the
  interval — instead of a count per batch (that line moved to `DEBUG`). The
  machinery is `LoopStats` from `tkati-core`; this node supplies its own phase
  names. The in/out delta is labelled `dropped` rather than `deduped`, since
  `LoopStats` is shared with nodes that don't deduplicate. See the README
* Added `benchmarks/bench_store.py` for A/B testing store tuning
* Fixed: a bucket that failed to open at startup was never cleaned up and sat
  on disk until the next restart

# 0.3.0

* Initial implementation of `tkati-node-dedup`: a Kafka-to-Kafka node that
  deduplicates events by a configurable field within a rolling
  processing-time window (e.g. "dedup by uid on a 3 hour window")
* Dedup state is tracked in an embedded RocksDB store, partitioned into
  hourly on-disk buckets that are automatically cleaned up once they age out
  of the window
* At-least-once delivery: a key is only marked seen after a confirmed,
  flushed produce, and the input offset is only committed after that — on any
  crash or dedup-store failure the node prefers forwarding a possible
  duplicate over dropping a real event
