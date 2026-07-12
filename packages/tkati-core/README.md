# tkati-core

`tkati-core` provides the building blocks for streaming data pipeline nodes that
read from Kafka and write to Kafka or ClickHouse.

## Settings

General form of settings is:

```toml
[input.topic]
# definition of input stream:
# - broker
# - topic name
# - message schema
# - message format = "json" / "arrow-batch"

[input.consumer]
# parameters local to this consumer
# - group_id
# - batch_size
# - batch_timeout_sec
# - auto_offset_reset

[output.topic]
# definition of output stream
# - broker
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
interfaces a node's input and output are built against. `KafkaConsumer` is the only
`Consumer` implementation today; `KafkaProducer` and `ClickhouseProducer` both
implement `Producer`. This is what lets a generic node pick its input/output kind
from config instead of hardcoding a concrete class.

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

settings = AppSettings()
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
[output.topic]
broker = "localhost:9092"
name = "my-output-topic"
key_column = "customer_id"   # column whose value becomes the Kafka message key
```

`key_column` is optional. When omitted (or `None`), messages are produced without a key.
When set, the value of that column for each row is used as the Kafka message key
(JSON format only — ignored for `"arrow-batch"`). This determines which Kafka partition
each message is routed to.
