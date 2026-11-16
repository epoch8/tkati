# tkati-node-el — generic extract/load node

Reads batches from a configurable input and writes them to a configurable output. Offsets are committed only after the batch is written and confirmed delivered (`produce_arrow` followed by a blocking `flush`), so delivery is at-least-once.

The loop is `tkati-core`'s `PipelinedNode`: it reads the next batch while producing the current one, and doesn't wait for a batch's delivery before moving on. Batches are still committed only once delivered, in read order (up to `[pipeline] max_in_flight` batches, default 4, may be waiting). On SIGTERM or SIGINT the node finishes the batch in hand, waits for everything in flight to be delivered, commits it and exits. A second signal forces an exit, leaving uncommitted batches to be read again on restart.

Input and output kinds are selected via the `type` field in each section — pick from whatever `tkati-core` supports. Every backend's settings split a **`connection`** tier (server-specific: how to reach the broker/database) from the resource tier (`topic` for Kafka, `table` for ClickHouse) and, where relevant, a tier local to this reader/writer instance (Kafka's `consumer` settings).

- **Input**: `"kafka"` (JSON or Arrow-batch messages from a Kafka/Redpanda topic)
- **Output**: `"kafka"` or `"clickhouse"` (native Arrow insert)
- **DLQ**: same `OutputSettings` shape as `output` — a DLQ can be Kafka or ClickHouse too
- **Transform** (optional): a Python function run on every batch between reading and writing; see [Transform](#transform)

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

# Optional. Prometheus endpoint, ON by default; these are the defaults.
[metrics]
enabled = true
port    = 8000
addr    = "0.0.0.0"
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

# Optional. Any librdkafka property, passed through to the producer; see
# "Client configuration" in tkati-core's README. `[input.config]` and
# `[dlq.config]` do the same for the consumer and a Kafka DLQ.
[output.config]
"compression.type" = "zstd"
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

## Transform

An optional `[transform]` section holds Python code that the node runs on every
batch between reading and writing: to cast fields the producer writes in the
wrong type, fill defaults, compute columns, drop columns or filter rows.

```toml
[transform]
code = '''
import polars as pl
import pyarrow as pa

def transform(batch: pa.Table) -> pa.Table:
    return pl.from_arrow(batch).drop("local_ip").to_arrow()
'''
```

Use a TOML multi-line literal string (`'''`): it takes Python verbatim, with no
escaping of backslashes or quotes. Like any setting, it can also come from the
environment, as `TRANSFORM__CODE`.

The code runs **once, at startup**, as a module of its own, and must define:

```python
def transform(batch: pa.Table) -> pa.Table: ...
```

A syntax error, an exception at top level, or no callable `transform` stops
the node at startup, before it connects to anything. Tracebacks name the code
`<settings: transform.code>`, with line numbers counted from the first line of
the string.

- `batch` holds the columns of `[input.topic.schema]`, with their Arrow types.
  Fields the schema doesn't name never reach the transform, so a computed
  column's inputs must be declared there.
- It must return a `pa.Table` (anything else fails the batch with a
  `TypeError`). It may return fewer rows (they count as `dropped`), none (the
  batch is committed and nothing is sent), or more.
- It may add, drop, rename, reorder and retype columns. For a ClickHouse output
  the returned columns are the insert's column list, so columns it omits get
  the table's `DEFAULT`; for a Kafka output they are the JSON keys.
- **An exception fails the batch**: it is rewound, not committed and not sent
  to the DLQ, and the node exits. On restart it reads the same batch again.
- The node logs the output schema on the first batch, and warns whenever it
  differs from the previous batch's. A schema that flips between batches is
  almost always a bug (e.g. `pa.array` inferring `null` for an all-`None`
  column).
- With the pipelined loop, earlier batches may still be in flight while the
  next is transformed: don't keep references to batches across calls.

[Polars](https://pola.rs) ships with the node and is the recommended way to
write one: `pl.from_arrow(batch)` and `df.to_arrow()` don't copy the data, and
its expressions run in parallel without holding the GIL. `pyarrow.compute`
works just as well. Polars returns strings as `large_string`; both outputs
accept it.

### Example

Type adaptation, defaults and computed columns, on a stream whose producers
write timestamps as ISO-8601 and `package_id` as a string:

```toml
[input.topic.schema]
time        = "string"        # producers write ISO-8601; read as text
package_id  = "string"        # producers write "7", not 7
country     = "string"
traffic_in  = "uint32"
traffic_out = "uint32"
local_ip    = "string"
# ...

[transform]
code = '''
import datetime as dt

import polars as pl
import pyarrow as pa

DEFAULT_COUNTRY = "ZZ"


def transform(batch: pa.Table) -> pa.Table:
    df = pl.from_arrow(batch)
    now = dt.datetime.now(dt.UTC)
    df = (
        df.with_columns(
            # Type adaptation. strict=False turns junk into null instead of
            # failing the batch; leave it strict where junk should stop the node.
            pl.col("time").str.to_datetime(time_unit="ms"),
            pl.col("package_id").cast(pl.Int32, strict=False),
            # Defaults: a missing field arrives as null.
            pl.col("country").fill_null(DEFAULT_COUNTRY),
            # Computed columns. Widen first: UInt32 + UInt32 wraps silently.
            traffic_total=pl.col("traffic_in").cast(pl.UInt64)
            + pl.col("traffic_out").cast(pl.UInt64),
            ingested_at=pl.lit(now, dtype=pl.Datetime("ms", "UTC")),
        )
        .with_columns(event_date=pl.col("time").dt.date())
        # Filter rows, drop columns the target doesn't have.
        .filter(pl.col("traffic_total") > 0)
        .drop("local_ip")
    )
    return df.to_arrow()
'''
```

Through a Kafka output, three input rows — one with `package_id` `"x"`, one
with zero traffic — come out as (`"x"` became null, the zero-traffic row was
filtered, `4294967295 + 1` did not wrap):

```json
{"time":"2026-10-05T12:00:00.123000","package_id":7,"country":"RU","traffic_in":4294967295,"traffic_out":1,"traffic_total":4294967296,"ingested_at":"2026-10-05T16:12:47.222000+00:00","event_date":"2026-10-05"}
{"time":"2026-10-05T12:00:02","package_id":null,"country":"DE","traffic_in":5,"traffic_out":1,"traffic_total":6,"ingested_at":"2026-10-05T16:12:47.222000+00:00","event_date":"2026-10-05"}
```

### Writing a transform

- **Strict casts fail the whole batch.** Polars casts are strict by default:
  `pl.col("package_id").cast(pl.Int32)` on `"x"` raises, which stalls the node
  on that batch. Decide per field whether junk means "null or a default"
  (`strict=False`, then `fill_null`) or "stop" (strict). The same holds for
  `str.to_datetime`, which also infers the format from the data: pass
  `format=` when producers are known to mix spellings.
- **Overflow is silent.** Arithmetic on narrow integer types wraps; widen
  first, as `traffic_total` does above.
- **Stay columnar.** Polars expressions, not `map_elements` or a Python loop
  over rows; those work, and show up as the `transform` phase.
- **Top-level code runs once**, at startup: put constants and expensive setup
  there, not in `transform`.
- **Test it** by loading the settings:
  `load_transform(AppSettings().transform)` (from `tkati_node_el.transform`)
  returns the same function the node calls, to run against a hand-built
  `pa.table({...})` in pytest, no broker needed.
- Libraries beyond Polars and pyarrow (DuckDB, a geo-IP reader) are the
  image's business: build `FROM` the node's image and install them.

## Metrics and perf log

Every 10 seconds the node logs where its wall clock went, using `LoopStats`
from `tkati-core`:

```
perf over 10s: 157000 rows in, 157000 out (0 dropped), 157 iterations (0 input-starved)
perf loop: wait/input=0.21s (2%) producer/serialize=2.10s (21%) producer/enqueue=0.35s (4%) producer/deliver=0.00s (0%) wait/in-flight=1.12s (11%) commit=0.38s (4%)
perf read: consumer/poll=4.43s (44%) consumer/parse=0.48s (5%) wait/loop=5.20s (52%)
```

Only a [transform](#transform) drops rows; without one `dropped` is always 0.

`perf loop:` is the node's loop, in the order its phases happen for a batch.
`perf read:` is its read-ahead thread, which reads the next batches while the
loop works on this one. The two lines run at the same time, so add up
percentages within a line, never across them.

* `transform`: the `[transform]` function, only on the line when one is
  configured. High with low `wait/input` means the transform is the
  bottleneck. A transform that loops over rows in Python also holds the GIL
  against the read-ahead thread, so `consumer/parse` slows down with it.
* `consumer/poll`, `consumer/parse`: fetching message batches from the broker,
  and decoding them into an Arrow table.
* `producer/serialize`, `producer/enqueue`: encoding rows into the output's
  wire format, and handing them to librdkafka.
* `producer/deliver`: a ClickHouse output records its whole insert here,
  retries and DLQ fallback included. A Kafka output doesn't wait for acks per
  batch, so it reads 0; see `wait/in-flight`.
* `commit`: the offset commit.
* `wait/input`: time the loop waited for the next batch from its read-ahead
  thread. High means the node is input-bound.
* `wait/in-flight`: time `done()` waited because `[pipeline] max_in_flight`
  batches were still undelivered. High means the node is output-bound.
* `wait/loop` (read line): time the read-ahead thread waited for the loop to
  take what it had read. High, with low `wait/input` and `wait/in-flight`,
  means encoding the output is the bottleneck.

Percentages are of the interval, so they **do not sum to 100**; the remainder
of a line is time in none of its named phases. `input-starved` counts iterations that
drained the topic and waited out the batch timeout. A mostly-starved interval
shows `consumer/poll` near 100% and says nothing about whether the node can
keep up. `tkati-core`'s README covers the phases in more depth.

The `perf over` and `perf loop:` numbers are served as Prometheus metrics at
`:8000/metrics` (the read line's are log-only). To turn
that off, set `[metrics] enabled = false` or the env var
`METRICS__ENABLED=false`; `METRICS__PORT` moves it. See `tkati-core`'s README
for the metric names and the PromQL that reproduces the log line's
percentages.

## DLQ semantics

DLQ *fallback triggering* is currently only implemented for the `clickhouse` output kind — `KafkaProducer` has no retry/split logic of its own. The DLQ *sink* itself (where isolated bad rows end up) can be Kafka or ClickHouse, independent of the primary output.

The fallback runs only when ClickHouse rejects a batch **because of the data** — a parse or value error, identified by the server's error code. Then the app switches to a recursive fallback to isolate the problematic rows:

1. The failing batch is split into `dlq_split_factor` equal sub-batches and each is inserted independently. Data errors are not retried, so no step of this waits.
2. If a sub-batch also fails it is split again — this repeats until individual rows are reached.
3. A single row that ClickHouse still rejects is written to the DLQ sink, preserving the full schema (Arrow IPC `arrow-batch` format for a Kafka DLQ).
4. After all rows are handled (inserted or DLQ'd), the input offset is committed and the app resumes normal large-batch processing.

**Any other insert failure fails the batch instead.** A connection error, a timeout, auth, or a schema error such as `TYPE_MISMATCH` is retried 3 times 1 second apart and then raised: the batch is rewound and re-read, and nothing goes to the DLQ. An outage means every row in the batch was fine, so quarantining them would be wrong, and a missed migration should stop the node rather than drain batches into the DLQ. See `design-docs/2026-09-28-clickhouse-error-classification.md`.

`dlq_split_factor` is a setting on the `clickhouse` `[output]` block (see above), not on `[dlq]` — it describes how the primary output retries, independent of where the DLQ sink sends isolated rows. With `dlq_split_factor=10` and a 1 000-row batch this takes at most 3 recursive levels (1000 → 100 → 10 → 1).

**Delivery guarantee: at-least-once.** If the recursion is abandoned part-way — the process crashes, or ClickHouse goes away mid-descent and the batch is failed — the uncommitted batch is re-read on restart and re-processed from the beginning, which may produce duplicate rows in the output and duplicate messages in the DLQ.
