# WIP 0.3.0

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
* Add `tkati_core.node` module: `InputSettings`/`OutputSettings` discriminated unions
  plus `build_consumer`/`build_producer` factories, so a generic node picks its
  input/output implementation from a settings object's `type` field without
  hardcoding a concrete class
* `Consumer`, `Producer`, `build_consumer`, and `build_producer` are now re-exported
  from the top-level `tkati_core` package
* **Breaking:** `Consumer.read_arrow`/`KafkaConsumer.read_arrow`/`read_pylist` params
  renamed: `aggregation_interval_seconds` → `timeout`, `max_events_to_aggregate` →
  `num_messages`

# 0.2.0

* Initial implementation of ClickhouseProducer

# 0.1.0

* Initial implementation of KafkaConsumer and KafkaProducer
