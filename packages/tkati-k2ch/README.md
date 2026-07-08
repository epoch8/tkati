# k2ch — Kafka to ClickHouse

Reads batches of JSON messages from a Kafka topic and inserts them into a ClickHouse table using the native Arrow protocol. Offsets are committed only after a successful insert (at-least-once delivery).

## Configuration

Settings are loaded from a TOML file. Set the `SETTINGS_FILE` environment variable to point to it (defaults to `settings.toml`).

```toml
[input.topic]
broker = "redpanda:29092"
name   = "traffic_event"

[input.topic.schema]
uid        = "string"
time       = "timestamp[ms]"
traffic_in = "uint32"
# … other columns

[input.consumer]
group_id          = "k2ch-group"
batch_size        = 1000
batch_timeout_sec = 10
auto_offset_reset = "latest"

[output]
host     = "clickhouse"
port     = 9000
user     = "default"
password = ""
database = "default"
table    = "traffic_event"
secure   = false

[dlq]
topic        = "k2ch-dlq"
# broker defaults to input.topic.broker when omitted
split_factor = 10
```

## DLQ semantics

When a batch insert fails after all retries, the app switches to a recursive fallback to isolate the problematic rows:

1. The failing batch is split into `split_factor` equal sub-batches and each is retried independently.
2. If a sub-batch also fails it is split again — this repeats until individual rows are reached.
3. A single row that ClickHouse still rejects is written to the DLQ Kafka topic in Arrow IPC (`arrow-batch`) format, preserving the full schema.
4. After all rows are handled (inserted or DLQ'd), the Kafka offset is committed and the app resumes normal large-batch processing.

With `split_factor=10` and a 1 000-row batch this takes at most 3 recursive levels (1000 → 100 → 10 → 1).

**Delivery guarantee: at-least-once.** If the process crashes mid-recursion the uncommitted batch is re-read on restart and re-processed from the beginning, which may produce duplicate rows in ClickHouse and duplicate messages in the DLQ topic.
