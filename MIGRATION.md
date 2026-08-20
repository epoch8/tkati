# Migrating from v0.2.0 to v0.3.0

At v0.2.0 the workspace shipped a single library package, `tkati-core`, plus
an unused placeholder `main.py` at the repo root. v0.3.0 turns it into a
small ecosystem: `tkati-core` (library) and two installable, runnable
pipeline nodes built on top of it — `tkati-node-el` (generic Kafka/ClickHouse
extract-load) and `tkati-node-dedup` (Kafka-to-Kafka deduplication). The
placeholder `main.py` was removed.

## Breaking changes in `tkati-core`

### 1. Settings restructured: connection info split out of topic/table settings

**Kafka** — `KafkaTopicSettings.broker` was removed. `KafkaInputSettings` and
`KafkaOutputSettings` now require a `connection: KafkaConnectionSettings`
block instead, and both gained a `type: Literal["kafka"] = "kafka"`
discriminator field.

- Before: `input.topic.broker`
- After: `input.connection.broker`

**ClickHouse** — `ClickHouseOutputSettings` was flat (`host`, `port`, `user`,
`password`, `database`, `table: str`, `secure`). It's now split into
`connection: ClickHouseConnectionSettings` (`host`/`port`/`user`/`password`/`secure`)
and `table: ClickHouseTableSettings` (`database`/`name`), plus a
`type: Literal["clickhouse"] = "clickhouse"` discriminator and a new
`dlq_split_factor: int = 10` field.

- Before: `output.host`, `output.database`, `output.table` (a string)
- After: `output.connection.host`, `output.table.database`, `output.table.name`

In TOML config, this means:

```toml
# before
[output]
host     = "clickhouse"
database = "default"
table    = "traffic_event"

# after
[output]
type = "clickhouse"

[output.connection]
host = "clickhouse"

[output.table]
database = "default"
name     = "traffic_event"
```

### 2. `KafkaConsumer` batch-read parameters renamed

`read_arrow`, `read_pylist`, and the internal `_consume_batch` all renamed
their parameters:

- `aggregation_interval_seconds` → `timeout`
- `max_events_to_aggregate` → `num_messages`

Update any call sites passing these as keyword arguments.

### 3. `KafkaProducer.from_topic_settings` takes a connection argument now

- Before: `KafkaProducer.from_topic_settings(topic)`
- After: `KafkaProducer.from_topic_settings(connection, topic)`

This mirrors the settings split in point 1. `from_output_settings(settings)`
is unaffected — it still takes just the settings object.

### 4. `ClickhouseProducer.from_output_settings` no longer takes `split_factor`

`split_factor` is now read from the new `settings.dlq_split_factor` field
(default `10`) instead of being passed as a keyword argument to the factory.
`ClickhouseProducer.__init__` still accepts `split_factor` directly if
you're constructing one by hand instead of via `from_output_settings`.

### 5. New `Consumer`/`Producer` base classes and factory functions

`tkati_core.consumer.Consumer` and `tkati_core.producer.Producer` are new
ABCs; `KafkaConsumer`, `KafkaProducer`, and `ClickhouseProducer` all now
inherit from them.

`tkati_core` gained a real top-level `__init__.py` (previously absent —
`from tkati_core import ...` didn't work at all; you had to import from
submodules directly, e.g. `tkati_core.kafka.consumer.KafkaConsumer`, which
still works). It now re-exports:

```python
from tkati_core import Consumer, Producer, InputSettings, OutputSettings, build_consumer, build_producer
```

- `build_consumer(settings: InputSettings) -> Consumer` and
  `build_producer(settings: OutputSettings, dlq_producer=None) -> Producer`
  dispatch on the settings object's `type` field to construct the right
  concrete consumer/producer — this is what `tkati-node-el` and
  `tkati-node-dedup` use internally, and is the recommended way to build
  consumers/producers from settings going forward.
- `InputSettings` (= `KafkaInputSettings`) and `OutputSettings`
  (`KafkaOutputSettings | ClickHouseOutputSettings`, discriminated on
  `type`) are new type aliases for building your own settings models on top
  of `tkati-core`.

### 6. `tkati-core` build backend changed

`tkati-core` now builds with `uv_build` instead of `setuptools.build_meta`.
If you build or publish `tkati-core` from source outside of `uv`, update
your tooling accordingly. `uv.lock` and `.python-version` are now gitignored
rather than committed.

## New in v0.3.0

### `tkati-node-el` — generic extract/load node

An installable package with a `tkati-node-el` CLI entry point: reads batches
from a configurable Kafka input and writes them to a configurable Kafka or
ClickHouse output (with an optional DLQ), driven entirely by a
`settings.toml` file using the settings types above. See
[`packages/tkati-node-el/README.md`](packages/tkati-node-el/README.md).

### `tkati-node-dedup` — Kafka-to-Kafka dedup node

An installable package with a `tkati-node-dedup` CLI entry point:
deduplicates a Kafka stream by a configurable field over a rolling
processing-time window, backed by an embedded, auto-expiring RocksDB store.
See [`packages/tkati-node-dedup/README.md`](packages/tkati-node-dedup/README.md)
and the corresponding entries in [`CHANGELOG.md`](CHANGELOG.md).

## Suggested migration steps

1. Update `KafkaInputSettings`/`KafkaOutputSettings`/`ClickHouseOutputSettings`
   construction (TOML config or Python) to the new `connection`/`table`
   nesting, and add the `type` discriminator field.
2. Update any direct calls to `KafkaConsumer.read_arrow`/`read_pylist` using
   the old `aggregation_interval_seconds`/`max_events_to_aggregate` keyword
   names.
3. Update any direct calls to `KafkaProducer.from_topic_settings` to pass
   `connection` as the first argument.
4. If you were passing `split_factor` to
   `ClickhouseProducer.from_output_settings`, move it to `dlq_split_factor`
   in your `ClickHouseOutputSettings` config instead.
5. If you build `tkati-core` from source with `setuptools`, switch to
   `uv build`.
6. Consider replacing any bespoke Kafka extract-load or dedup scripts with
   `tkati-node-el` / `tkati-node-dedup` now that they exist as ready-made,
   configurable nodes.
