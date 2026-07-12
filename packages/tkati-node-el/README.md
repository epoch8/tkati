# tkati-node-el — generic extract/load node

Reads batches from a configurable input and writes them to a configurable output. Offsets are committed only after a successful write (at-least-once delivery).

Input and output kinds are selected via the `type` field in each section — pick from whatever `tkati-core` supports:

- **Input**: `"kafka"` (JSON or Arrow-batch messages from a Kafka/Redpanda topic)
- **Output**: `"kafka"` or `"clickhouse"` (native Arrow insert)

## Configuration

Settings are loaded from a TOML file. Set the `SETTINGS_FILE` environment variable to point to it (defaults to `settings.toml`).

```toml
[input]
type = "kafka"

[input.topic]
broker = "redpanda:29092"
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
type     = "clickhouse"
host     = "clickhouse"
port     = 9000
user     = "default"
password = ""
database = "default"
table    = "traffic_event"
secure   = false

[dlq]
topic        = "node-el-dlq"
# broker defaults to input.topic.broker when omitted
split_factor = 10
```

A Kafka output instead looks like:

```toml
[output]
type = "kafka"

[output.topic]
broker = "redpanda:29092"
name   = "some-other-topic"
format = "json"        # or "arrow-batch"
key_column = "uid"     # optional
```

## DLQ semantics

DLQ is currently only meaningful for the `clickhouse` output kind. When a batch insert fails after all retries, the app switches to a recursive fallback to isolate the problematic rows:

1. The failing batch is split into `split_factor` equal sub-batches and each is retried independently.
2. If a sub-batch also fails it is split again — this repeats until individual rows are reached.
3. A single row that ClickHouse still rejects is written to the DLQ Kafka topic in Arrow IPC (`arrow-batch`) format, preserving the full schema.
4. After all rows are handled (inserted or DLQ'd), the input offset is committed and the app resumes normal large-batch processing.

With `split_factor=10` and a 1 000-row batch this takes at most 3 recursive levels (1000 → 100 → 10 → 1).

**Delivery guarantee: at-least-once.** If the process crashes mid-recursion the uncommitted batch is re-read on restart and re-processed from the beginning, which may produce duplicate rows in ClickHouse and duplicate messages in the DLQ topic.
