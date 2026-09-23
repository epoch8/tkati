# tkati-core

`tkati-core` provides the building blocks for streaming data pipeline nodes that
read from Kafka and write to Kafka or ClickHouse.

## Settings

Every backend's settings split into a **`connection`** tier (server-specific: how to
reach the broker/database) and a resource tier named for what that backend calls the
thing you read/write (`topic` for Kafka, `table` for ClickHouse) — plus, where relevant,
a tier for behavior local to this particular reader/writer (Kafka's `consumer` settings).
This keeps server-specific config separate from per-instance client config, and is
meant to stay consistent as more backends (e.g. RabbitMQ) are added.

```toml
[input]
type = "kafka"

[input.connection]
broker = "localhost:9092"

[input.topic]
# definition of input stream:
# - topic name
# - message schema
# - message format = "json" / "arrow-batch"

[input.consumer]
# parameters local to this consumer
# - group_id
# - batch_size
# - batch_timeout_sec
# - auto_offset_reset

[output]
type = "kafka"  # or "clickhouse"

[output.connection]
broker = "localhost:9092"

[output.topic]
# definition of output stream
# - topic name
# - message schema
# - message format = "json" / "arrow-batch"
# - key_column (optional) = column to use as the Kafka message key

[...]
# settings specific to node function
```

## Usage

### `Consumer` / `Producer` base classes

`tkati_core.consumer.Consumer` and `tkati_core.producer.Producer` are the abstract
interfaces a node's input and output are built against. A `Consumer` reads a batch
with `read_arrow` (an Arrow table, which fails the batch on a malformed message) or
`read_pylist` (a list of dicts, which skips and logs one). `KafkaConsumer` is the only
`Consumer` implementation today; `KafkaProducer` and `ClickhouseProducer` both
implement `Producer`. This is what lets a generic node pick its input/output kind
from config instead of hardcoding a concrete class.

### `LoopStats` — where a node's wall clock went

`tkati_core.stats.LoopStats` accumulates per-phase timings across a node's loop
and logs a breakdown on an interval (every 10s by default). The phase names,
the log prefix and the cadence are all constructor arguments, because nodes
have different pipelines — a dedup node has lookup and write phases an
extract/load node does not.

```python
from tkati_core import CONSUMER_PHASES, PRODUCER_PHASES, LoopStats

PHASES = (*CONSUMER_PHASES, *PRODUCER_PHASES, "commit")
stats = LoopStats(name="my-node", phases=PHASES)

while True:
    # Pass `stats` down and the consumer and producer time their own phases —
    # see below.
    batch = consumer.read_arrow(..., stats=stats)
    stats.iterations += 1
    if batch is None:
        stats.starved_iterations += 1
        continue
    stats.rows_in += len(batch)

    producer.produce_arrow(batch, stats=stats)
    producer.flush(stats=stats)
    stats.rows_out += len(batch)

    with stats.phase("commit"):
        consumer.commit()

    stats.report_if_due()
```

```
my-node perf over 10s: 157000 rows in, 153880 out (3120 dropped), 157 iterations (0 input-starved)
my-node perf: consumer/poll=4.43s (44%) consumer/parse=0.48s (5%) producer/serialize=2.10s (21%) producer/enqueue=0.35s (4%) producer/deliver=1.51s (15%) commit=0.38s (4%)
```

`Consumer.read_arrow` and `Consumer.read_pylist` take an optional `stats`
and split their own time into the two phases named by `CONSUMER_PHASES`:
**`consumer/poll`**, fetching from the broker, and **`consumer/parse`**, turning
the raw payloads into an Arrow table or dicts. These have unrelated fixes
(batch sizing and broker latency versus JSON decoding cost), so they are worth
telling apart.

`Producer.produce_arrow`, `produce_pylist` and `flush` do the same with the
three `PRODUCER_PHASES`:

* **`producer/serialize`**: encoding rows into the wire format (the wire-type
  cast, `to_pylist` and `orjson.dumps`, or the Arrow IPC write for
  `arrow-batch`). This is CPU time in Python.
* **`producer/enqueue`**: the per-message `produce()` calls that hand the
  encoded bytes to librdkafka. It scales with message count, so `arrow-batch`
  (one message per batch) all but removes it.
* **`producer/deliver`**: waiting for the sink to accept. For Kafka that is
  `flush()`. librdkafka starts sending in the background during `enqueue`, so
  this is the *remaining* wait for broker acks, not the batch's total network
  time. `ClickhouseProducer` records its whole insert here, retries and DLQ
  fallback included, because `clickhouse_connect` encodes and sends in a
  single call.

The prefixes mark these figures as timed inside `tkati-core`. A node's own
phases stay unprefixed. Splice the tuples into your phase tuple rather than
writing the names out by hand, so a rename in core can't leave your column
silently reading `0.00s`.

Do **not** also wrap these calls in a phase of your own: that would count the
same time twice and break the invariant below.

`phases` is an explicit ordered tuple, not derived from which phases happened
to fire: a phase that doesn't run during an interval's first iteration would
otherwise shift the column order between reports, and a stable order is what
makes two consecutive lines comparable.

Percentages are of the interval rather than of each other, so they do **not**
sum to 100 — the shortfall is time in none of the named phases, which keeps
unaccounted work visible. Track `starved_iterations` for iterations that were
blocked waiting on input: `consumer/poll` blocks until the batch fills or the timeout
expires, so on an under-fed node it approaches 100% and nothing else on the
line means anything.

### `tkati_core.settings` — generic node settings aliases

`tkati_core.settings` defines `InputSettings`/`OutputSettings` (discriminated unions
over every input/output kind `tkati-core` implements). Use those aliases with the
factory helpers in `tkati_core.consumer` and `tkati_core.producer`, or import the
helpers from the top-level `tkati_core` package for convenience.

```python
from tkati_core import InputSettings, OutputSettings, build_consumer, build_producer
from tkati_core.settings import TomlBaseSettings

class AppSettings(TomlBaseSettings):
    input: InputSettings
    output: OutputSettings

settings = AppSettings()
consumer = build_consumer(settings.input)
producer = build_producer(settings.output)
```

`build_producer` also takes an optional `dlq_producer` kwarg, forwarded to
`ClickhouseProducer.from_output_settings` when `settings.type == "clickhouse"` (a no-op
for the `"kafka"` output kind, which has no DLQ-fallback logic of its own). The
recursive-split batch size for that fallback comes from `settings.dlq_split_factor`
(a field on `ClickHouseOutputSettings` itself), not from a separate parameter.

### Constructing a consumer from settings

Use `KafkaConsumer.from_input_settings` to construct a consumer directly from
`KafkaInputSettings` — no need to manually map fields to Confluent Kafka config keys.

```python
from tkati_core.settings import TomlBaseSettings
from tkati_core.kafka.settings import KafkaInputSettings
from tkati_core.kafka.consumer import KafkaConsumer

class AppSettings(TomlBaseSettings):
    input: KafkaInputSettings
    # ...

settings = AppSettings()  # settings.input.connection.broker, settings.input.topic.name, ...
consumer = KafkaConsumer.from_input_settings(settings.input)

# Read a batch
table = consumer.read_arrow(
    aggregation_interval_seconds=settings.input.consumer.batch_timeout_sec,
    max_events_to_aggregate=settings.input.consumer.batch_size,
)
consumer.commit()
```

The factory method sets `enable.auto.commit=False` — offsets must be committed explicitly
via `consumer.commit()`.

### Constructing a producer from settings

Use `KafkaProducer.from_output_settings` to construct a producer directly from
`KafkaOutputSettings`. It accepts PyArrow tables or record batches and handles
serialization according to the topic's `format` setting.

```python
from tkati_core.settings import TomlBaseSettings
from tkati_core.kafka.settings import KafkaOutputSettings
from tkati_core.kafka.producer import KafkaProducer

class AppSettings(TomlBaseSettings):
    output: KafkaOutputSettings
    # ...

settings = AppSettings()
producer = KafkaProducer.from_output_settings(settings.output)

# Produce a PyArrow table (one message per row for "json" format)
producer.produce_arrow(table)
producer.flush()
producer.close()  # flushes and releases resources
```

`ClickhouseProducer.from_output_settings` (in `tkati_core.clickhouse.producer`) works the
same way against `ClickHouseOutputSettings`.

**Formats** — controlled by `output.topic.format` in `settings.toml`:

- `"json"` *(default)*: each row becomes a separate Kafka message serialized with orjson.
- `"arrow-batch"`: the entire table is serialized as a single Arrow IPC stream message.

**Message keys** — controlled by `output.topic.key_column` in `settings.toml`:

```toml
[output.connection]
broker = "localhost:9092"

[output.topic]
name = "my-output-topic"
key_column = "customer_id"   # column whose value becomes the Kafka message key
```

`key_column` is optional. When omitted (or `None`), messages are produced without a key.
When set, the value of that column for each row is used as the Kafka message key
(JSON format only — ignored for `"arrow-batch"`). This determines which Kafka partition
each message is routed to.
