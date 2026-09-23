# 0.5.0

* `KafkaConsumer` and `KafkaProducer` run on a native (Rust) extension,
  `tkati_core._native`, with librdkafka linked in statically. Their
  constructors, `from_*` classmethods, attributes, methods and `stats` phases
  are unchanged.
  * `produce_arrow` with `format="json"` encodes rows straight from the Arrow
    columns, in parallel across all cores, instead of via `to_pylist()` +
    orjson. This makes it about 30x faster, e.g. 7.4 s → 0.24 s per 1M rows of
    tkati-node-el's schema. Output is byte-identical for strings, integers,
    booleans, nulls, nested lists/structs and timestamps; float formatting
    differs (see below).
  * `read_arrow` builds its batch buffer natively as messages arrive and hands
    it to pyarrow's JSON reader zero-copy, split into blocks so that every core
    parses a share. It is 1.7–2.8x faster, e.g. 0.43 s → 0.23 s per 1M rows,
    and parsing semantics are unchanged.
  * Polling and enqueueing loop natively rather than calling into
    confluent-kafka once per message.
  * `read_pylist` and `produce_pylist` still use orjson. Building or reading
    Python dicts needs the GIL either way, and a native parse benchmarked
    slower.
  * The JSON encoder's parallelism follows `RAYON_NUM_THREADS` (default: all
    available cores).
* **Breaking:**
  * Kafka errors from `commit`, construction etc. raise
    `tkati_core._native.KafkaError`, not `confluent_kafka.KafkaException`.
  * `KafkaConsumer.consumer` / `KafkaProducer.producer` are now the native
    client objects, not confluent-kafka's.
  * Building tkati-core from source needs a Rust toolchain. Wheels are
    published for manylinux x86_64 and aarch64 (abi3, CPython ≥ 3.13).
* Behaviour changes:
  * `produce_*` no longer raises `BufferError` when librdkafka's local queue
    is full: it waits for the queue to drain and retries.
  * A tombstone (message with no value) in a `read_arrow` batch raises a
    `ValueError` naming how many there were, instead of a `TypeError`.
    `read_pylist` still skips and logs it.
  * `produce_arrow` JSON: floats are written in the shortest round-trip form
    with a `1.0e16`-style exponent (orjson wrote `1e+16`), and float32 values
    are no longer widened to float64 first. Decimal and binary columns are
    encoded, as a JSON number and a hex string respectively, where orjson
    raised `TypeError`.
* librdkafka logs still go to stderr, as with confluent-kafka.
* confluent-kafka remains a dependency, for `tkati_core.kafka.testing`'s
  `AdminClient`.

# 0.4.5

* `Producer.produce_arrow`, `produce_pylist` and `flush` take an optional
  `stats: LoopStats` and split their time into `producer/serialize`,
  `producer/enqueue` and `producer/deliver`. `KafkaProducer` now encodes a
  batch fully before enqueueing it. `ClickhouseProducer` records everything as
  `producer/deliver`
* Add `PRODUCER_PHASES`, re-exported from `tkati_core`
* `KafkaConsumer.read_pylist` takes `stats`, split like `read_arrow`
* `read_pylist` is now an abstract method on the base `Consumer`, so it can be
  called on whatever `build_consumer` returns. **Breaking** for any
  out-of-tree `Consumer` subclass, which must now implement it
* Add `tkati_core.metrics`: `LoopStatsCollector` exports a `LoopStats` as
  Prometheus counters (`tkati_phase_seconds_total{node,phase}`,
  `tkati_wall_seconds_total`, rows in/out, iterations, starved iterations), and
  `start_metrics_server(MetricsSettings, stats)` serves them. New dependency:
  `prometheus-client`
* `LoopStats.totals()` returns everything counted since creation, across
  reports. `report()`/`reset()` still restart the interval as before
* **Breaking for log parsers:** `CONSUMER_PHASES` is now
  `("consumer/poll", "consumer/parse")`. Phases timed inside core are prefixed
  with their component

# 0.4.4

* `Consumer.read_arrow` takes an optional `stats: LoopStats` and splits its own
  wall clock into `poll` (fetching from the broker) and `parse` (decoding into
  Arrow), so callers can tell broker latency apart from JSON cost
* Add `CONSUMER_PHASES` (`("poll", "parse")`, re-exported from `tkati_core`) for
  nodes to splice into their `LoopStats` phase tuple
* The consumer's per-batch log lines moved from `info` to `debug` — at
  production throughput they were tens of lines a second and buried the periodic
  perf report. Row-count mismatches and parse failures are unaffected

# 0.4.3

* Add `LoopStats` (`tkati_core.stats`, re-exported from `tkati_core`): per-phase
  wall-clock accounting for a node's main loop, logged on an interval (10s by
  default) as seconds and percent. Phase names, log prefix and cadence are
  constructor arguments so each node can describe its own pipeline. Moved here
  from `tkati-node-dedup`, which had it inline

# 0.3.0

* Add shared `Producer` base class implemented by `KafkaProducer` and `ClickhouseProducer`
* `ClickhouseProducer` now supports `produce_pylist`, `flush`, and `close`, and
  can be used as `dlq_producer` for another `ClickhouseProducer`
* Add shared `Consumer` base class, implemented by `KafkaConsumer`
* **Breaking:** settings now split server-specific config into its own `connection`
  tier, separate from resource identity and client-local behavior:
  * `KafkaTopicSettings.broker` moved to new `KafkaConnectionSettings.broker`;
    `KafkaInputSettings`/`KafkaOutputSettings` gain a required `connection` field
  * `ClickHouseOutputSettings` decomposed into `connection: ClickHouseConnectionSettings`
    (`host`/`port`/`user`/`password`/`secure`) and `table: ClickHouseTableSettings`
    (`database`/`name` — the table-name field is now `name`, not `table`)
  * `KafkaInputSettings`/`KafkaOutputSettings`/`ClickHouseOutputSettings` each gain a
    `type` discriminator field (`"kafka"`/`"clickhouse"`), letting callers select
    input/output kind from config via a discriminated union instead of hardcoding a
    concrete type
  * `KafkaProducer.from_topic_settings` now takes `(connection, topic)` instead of
    just `(topic)`
* Add `InputSettings`/`OutputSettings` discriminated unions to `tkati_core.settings`,
  plus `build_consumer` (in `tkati_core.consumer`) and `build_producer` (in
  `tkati_core.producer`) factories, so a generic node picks its input/output
  implementation from a settings object's `type` field without hardcoding a
  concrete class
* `Consumer`, `Producer`, `InputSettings`, `OutputSettings`, `build_consumer`, and
  `build_producer` are now re-exported from the top-level `tkati_core` package
* **Breaking:** `Consumer.read_arrow`/`KafkaConsumer.read_arrow`/`read_pylist` params
  renamed: `aggregation_interval_seconds` → `timeout`, `max_events_to_aggregate` →
  `num_messages`
* **Breaking:** `ClickHouseOutputSettings` gains a `dlq_split_factor: int = 10` field;
  `build_producer` no longer takes a `split_factor` kwarg — it derives the value from
  `settings.dlq_split_factor` when `settings` is a `ClickHouseOutputSettings`

# 0.2.0

* Initial implementation of ClickhouseProducer

# 0.1.0

* Initial implementation of KafkaConsumer and KafkaProducer
