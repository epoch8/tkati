# tkati-node-el — generic extract/load node

Reads batches from a configurable input and writes them to a configurable output. Offsets are committed only after a successful write (at-least-once delivery).

Input and output kinds are selected via the `type` field in each section — pick from whatever `tkati-core` supports. Every backend's settings split a **`connection`** tier (server-specific: how to reach the broker/database) from the resource tier (`topic` for Kafka, `table` for ClickHouse) and, where relevant, a tier local to this reader/writer instance (Kafka's `consumer` settings).

- **Input**: `"kafka"` (JSON or Arrow-batch messages from a Kafka/Redpanda topic)
- **Output**: `"kafka"` or `"clickhouse"` (native Arrow insert)
- **DLQ**: same `OutputSettings` shape as `output` — a DLQ can be Kafka or ClickHouse too

## Configuration

Settings are loaded from a TOML file. Set the `SETTINGS_FILE` environment variable to point to it (defaults to `settings.toml`).

```toml
[input]
type = "kafka"

[input.connection]
broker = "redpanda:29092"

[input.topic]
name   = "traffic_event"

[input.topic.schema]
uid        = "string"
time       = "timestamp[ms]"
traffic_in = "uint32"
# … other columns

[input.consumer]
group_id          = "node-el-group"
batch_size        = 1000
batch_timeout_sec = 10
auto_offset_reset = "latest"

[output]
type             = "clickhouse"
dlq_split_factor = 10

[output.connection]
host     = "clickhouse"
port     = 9000
user     = "default"
password = ""
secure   = false

[output.table]
database = "default"
name     = "traffic_event"

[dlq]
type = "kafka"

[dlq.connection]
broker = "redpanda:29092"

[dlq.topic]
name = "node-el-dlq"
```

A Kafka output instead looks like:

```toml
[output]
type = "kafka"

[output.connection]
broker = "redpanda:29092"

[output.topic]
name   = "some-other-topic"
format = "json"        # or "arrow-batch"
key_column = "uid"     # optional
```

A ClickHouse DLQ instead looks like:

```toml
[dlq]
type = "clickhouse"

[dlq.connection]
host     = "clickhouse"
port     = 9000
user     = "default"
password = ""
secure   = false

[dlq.table]
database = "default"
name     = "traffic_event_dlq"
```

## DLQ semantics

DLQ *fallback triggering* is currently only implemented for the `clickhouse` output kind — `KafkaProducer` has no retry/split logic of its own. The DLQ *sink* itself (where isolated bad rows end up) can be Kafka or ClickHouse, independent of the primary output. When a batch insert fails after all retries, the app switches to a recursive fallback to isolate the problematic rows:

1. The failing batch is split into `dlq_split_factor` equal sub-batches and each is retried independently.
2. If a sub-batch also fails it is split again — this repeats until individual rows are reached.
3. A single row that ClickHouse still rejects is written to the DLQ sink, preserving the full schema (Arrow IPC `arrow-batch` format for a Kafka DLQ).
4. After all rows are handled (inserted or DLQ'd), the input offset is committed and the app resumes normal large-batch processing.

`dlq_split_factor` is a setting on the `clickhouse` `[output]` block (see above), not on `[dlq]` — it describes how the primary output retries, independent of where the DLQ sink sends isolated rows. With `dlq_split_factor=10` and a 1 000-row batch this takes at most 3 recursive levels (1000 → 100 → 10 → 1).

**Delivery guarantee: at-least-once.** If the process crashes mid-recursion the uncommitted batch is re-read on restart and re-processed from the beginning, which may produce duplicate rows in the output and duplicate messages in the DLQ.
