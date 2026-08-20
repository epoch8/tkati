# Changelog

One entry per jj change, keyed by its change identifier (stable across
`jj describe`/`jj squash`/rebases — use `jj log -r <change-id>` to look one
up). Newest first.

## 0.3.0

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
