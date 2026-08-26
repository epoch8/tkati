# Changelog

One entry per jj change, keyed by its change identifier (stable across
`jj describe`/`jj squash`/rebases — use `jj log -r <change-id>` to look one
up). Newest first.

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
