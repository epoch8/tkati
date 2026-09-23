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
