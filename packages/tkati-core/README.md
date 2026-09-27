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

### `SyncNode` — the worker loop harness

`tkati_core.SyncNode` runs a node's loop. It owns the input, the output, the DLQ,
`LoopStats`, the metrics server, signal handling and shutdown. Node code is a
`for` loop over `node.consume_arrow()` or `node.consume_pylist()`, which
yield a `Batch` for each batch read and `Idle` when a poll comes back empty.

```python
from tkati_core import Batch, NodeSettings, SyncNode


class AppSettings(NodeSettings): ...  # the node's own sections


def run(node: SyncNode) -> None:
    for event in node.consume_arrow():
        if isinstance(event, Batch):
            node.done(event, output_arrow=transform(event.data))


def main() -> None:
    with SyncNode.from_settings(AppSettings()) as node:
        run(node)
```

**A node finishes each batch with `node.done(event, output_arrow=table)`.**
`done()` sends the output, waits until it's delivered, commits the batch, and
returns once the commit is made. There is no separate send, so the output is
always tied to the input batch it came from. No output, or an empty one,
sends nothing. Work that must follow delivery, or must not repeat if the batch
is re-read (marking keys seen, counting what was dropped), goes on the lines
after it.

**Arrow or dicts.** The node names both formats explicitly:

- **Input.** `consume_arrow()` reads with `Consumer.read_arrow`: `event.data`
  is a `pa.Table`, and a message that fails to decode fails its whole batch.
  `consume_pylist()` reads with `Consumer.read_pylist`: `event.data` is a
  `list[dict]`, and a message that fails to decode is logged and skipped.
  The node itself isn't iterable, so the choice is always visible.
- **Output.** `done(event, output_arrow=table)` sends with
  `Producer.produce_arrow`, and `done(event, output_pylist=rows)` with
  `produce_pylist`. It's independent of the input format: a
  `consume_pylist()` node may send `output_arrow=`. At most one of
  `output_arrow`, `output_pylist` and `rows_out` may be given.

- **Batch as read.** What's committed is always the batch as read, whatever
  the node filtered out of `event.data`.
- **A forgotten `done()`.** Asking for the next event while a batch isn't done
  raises `RuntimeError`. The batch is rewound, so the bug is loud instead of
  showing up as consumer lag. `done()` twice raises too.
- **Exceptions.** An exception before or inside `done()` (for example a failed
  send or flush) rewinds the batch (`Consumer.rewind`) and propagates. After `done()`
  the batch is committed, so there is nothing to rewind. `break` and
  `KeyboardInterrupt` before `done()` leave the batch uncommitted without
  waiting on a seek. Either way it is read again after a restart.
- **Later.** `done()` being synchronous is a first step. A later version will
  pipeline the loop: `done()` will return before delivery and take callbacks
  for the work that follows.

**Shutdown.** `from_settings` makes SIGTERM and SIGINT stop the node after the
current batch. A signal that arrives while the node is waiting on a poll cuts
the poll short. A second signal forces an exit with `KeyboardInterrupt`. On
exit the node closes the consumer, the output and the DLQ, in that order. The
node's own resources belong in the same `with` statement.

**Stats.** The node keeps two sets of stats: `node.stats`, a `LoopStats` for
the loop thread, and `node.read_stats`, a `PhaseStats` for reading. It passes
the first to the producer and the second to the consumer, and counts
iterations, starved iterations and rows in and out itself. Rows in
and out are both counted when a batch is committed, so a pipelined node's
report never splits a batch across intervals. A node
times its own work with `node.phase("name")` and passes the full report order
as `phases=` (the default is `DEFAULT_PHASES`). That tuple must include every
phase the harness times on the loop thread, `wait/input` among them, and must
not include `CONSUMER_PHASES`, which are timed in `read_stats`.

The node's report has two phase lines, one per set of stats, for the same
interval. `perf loop:` is the loop thread:
waiting for the next batch (`wait/input`), the node's own phases, sending
(`producer/*`), waiting for a free in-flight slot (`wait/in-flight`, in a
`PipelinedNode`) and committing. `perf read:` is reading: `consumer/poll`,
`consumer/parse` and, with read-ahead, `wait/loop`, the reader waiting for the
loop to take what it read. Only the loop's stats are exported as metrics.

```
perf over 10s: 157000 rows in, 153880 out (3120 dropped), 157 iterations (0 input-starved)
perf loop: wait/input=0.21s (2%) lookup=0.52s (5%) producer/serialize=2.10s (21%) producer/enqueue=0.35s (4%) producer/deliver=0.00s (0%) wait/in-flight=5.12s (51%) commit=0.38s (4%) write=0.21s (2%)
perf read: consumer/poll=4.43s (44%) consumer/parse=0.48s (5%) wait/loop=5.02s (50%)
```

The three waits say where the bottleneck is. High `wait/input`: the input,
or reading and parsing it. High `wait/in-flight`: delivery. High `wait/loop`
with low loop waits: the loop's own work.

**Read-ahead.** `from_settings` reads the next batch in the background while
the loop body processes the current one, on a thread of the node's own. It is
configured by the `[pipeline]` section:

```toml
[pipeline]
read_ahead = 1   # batches read ahead; 0 reads on the loop thread, as before
```

Nothing changes for node code: `done()` still commits before it returns, and
it commits the batch's own offsets, never a batch still waiting in the queue.
A stop drops the batches read ahead without committing them, so they are read
again after a restart. Time the loop spends waiting for the reader is
`wait/input`. Time the reader spends waiting for room in the queue is
`wait/loop`, on the read line. Without read-ahead the loop reads itself, and
`wait/input` is the whole read. Constructing a `SyncNode` directly
defaults to `read_ahead=0`.

**`PipelinedNode`.** A sibling of `SyncNode` for nodes that shouldn't wait for
each batch's delivery. Its `done()` sends the output and returns at once; the
batch is committed later, on the loop thread, once its output and every
earlier batch's output is delivered, in read order. So **the lines after
`done()` run before the commit**. Work that must follow the commit goes in a
callback:

```python
from functools import partial

with PipelinedNode.from_settings(settings) as node:
    for event in node.consume_arrow():
        if isinstance(event, Batch):
            kept, keys = dedupe(event.data)
            node.done(event, output_arrow=kept, after_commit=partial(mark_seen, keys))
```

`after_commit` runs after that batch's commit, between events or inside a
later `done()`, never at the same time as the loop body, and never for a batch
that isn't committed. Up to `[pipeline] max_in_flight` finished batches
(default 4) may wait for delivery; past that, `done()` blocks until the oldest
is delivered and committed, timed as `wait/in-flight`. A failed delivery raises
`DeliveryError` from a later `done()` or event request, and the oldest
uncommitted batch is rewound. A clean exit, or a stop, waits for every
finished batch to be delivered and commits it; `KeyboardInterrupt` doesn't.

Choose `SyncNode` when the code after `done()` must see the batch committed,
or throughput doesn't matter; choose `PipelinedNode` when it does, and move
that code into `after_commit`. The two classes are deliberately unrelated
types, so a function written for one (`def run(node: SyncNode)`) doesn't
type-check with the other. `PipelinedNode` requires `wait/in-flight` in any
custom `phases=` tuple (`PIPELINED_PHASES` is its default).

Against a local Redpanda, node-el's loop moved 200k JSON rows in batches of
1000 at about 90k rows/s with `SyncNode`, 97k with read-ahead, and 128k with
`PipelinedNode` (`benchmarks/bench_node_pipeline.py`).

**Nodes without an output producer.** `NodeSettings.output` is optional. When
it's absent, the node has no producer (`producer=None`), and passing
`output_arrow=` or `output_pylist=` to `done()` raises. Such a node writes through its own client, for
example to a cloud API. It must finish those writes before it calls `done()`,
which commits the batch, and reports the rows it wrote with
`node.done(event, rows_out=n)`:

```python
def run(node: SyncNode, client: ApiClient) -> None:
    for event in node.consume_arrow():
        if isinstance(event, Batch):
            with node.phase("upload"):
                client.upload(event.data)  # returns once the API accepted it
            node.done(event, rows_out=len(event.data))


# phases=("wait/input", "upload", "commit"); SINK_PHASES is the default.
```

**Testing.** `tkati_core.testing.memory_node(batches)` returns a `SyncNode` over
an in-memory consumer and producer, along with those two doubles. They record
what was read, sent, flushed, committed and rewound, in one shared `log`, and
the loop ends once `batches` runs out. `memory_pipelined_node(batches,
deliver="manual")` does the same for a `PipelinedNode`, with a producer that
holds deliveries until the test calls `producer.release(tag)` (or
`producer.fail(tag, error)`), so a test can hold them back or release them out
of order; `producer.on_wait` runs whenever the node blocks on one. For tests
against a real broker, construct `SyncNode(consumer, producer, ...,
stop_when_idle=True)` (or `PipelinedNode`): it processes what is already in
the topic and stops at the first empty poll.

### `Consumer` / `Producer` base classes

`tkati_core.consumer.Consumer` and `tkati_core.producer.Producer` are the abstract
interfaces a node's input and output are built against. A `Consumer` reads a batch
with `read_arrow` (an Arrow table, which fails the batch on a malformed message) or
`read_pylist` (a list of dicts, which skips and logs one). Either returns a
`ConsumedBatch`, whose `.data` is the table or list, or `None` when nothing arrived.
Each batch then ends in one of two explicit calls, made in the order the batches
were read:

- `consumer.commit(batch)`: the batch is fully processed. Exactly its offsets are
  committed, even if later batches have already been read.
- `consumer.rewind(batch)`: processing failed. The consumer seeks back to where the
  batch started, so it is read again, and so is everything read after it. Those
  later batches can no longer be committed.

Committing or rewinding anything but the oldest outstanding batch raises
`ValueError`. Nothing is ever committed implicitly. `KafkaConsumer` is the only
`Consumer` implementation today; `KafkaProducer` and `ClickhouseProducer` both
implement `Producer`. This is what lets a generic node pick its input/output kind
from config instead of hardcoding a concrete class.

**Delivery.** `produce_arrow` and `produce_pylist` take an optional `tag=`,
which groups the messages you want to wait for together (the node harness tags
each input batch's output). `producer.wait_delivered(tag, timeout)` returns
`True` once all of them are acked, and `False` if `timeout` seconds pass
first; `None` waits as long as it takes, and `0` only checks. It raises
`DeliveryError` as soon as one of them failed, for example a message the
broker rejected or librdkafka gave up on. `flush()` alone doesn't tell you
that: it returns once nothing is in flight, delivered or not.
`ClickhouseProducer` inserts synchronously, so its `wait_delivered` is always
`True`.

### `LoopStats` — where a node's wall clock went

`tkati_core.stats.LoopStats` accumulates per-phase timings across a node's loop
and logs a breakdown on an interval (every 10s by default). The phase names,
the log prefix and the cadence are all constructor arguments, because nodes
have different pipelines — a dedup node has lookup and write phases an
extract/load node does not.

A `SyncNode` does all of this for you. The loop below is what it runs, and is for
code that drives a consumer and producer by hand.

```python
from tkati_core import CONSUMER_PHASES, PRODUCER_PHASES, LoopStats

PHASES = (*CONSUMER_PHASES, *PRODUCER_PHASES, "commit")
stats = LoopStats(phases=PHASES)

while True:
    # Pass `stats` down and the consumer and producer time their own phases —
    # see below.
    batch = consumer.read_arrow(..., stats=stats)
    stats.iterations += 1
    if batch is None:
        stats.starved_iterations += 1
        continue

    try:
        producer.produce_arrow(batch.data, stats=stats)
        producer.flush(stats=stats)
    except Exception:
        consumer.rewind(batch)
        raise

    with stats.phase("commit"):
        consumer.commit(batch)
    # Both counted once committed, so in - out is the batch's drops.
    stats.rows_in += len(batch.data)
    stats.rows_out += len(batch.data)

    stats.report_if_due()
```

```
perf over 10s: 157000 rows in, 153880 out (3120 dropped), 157 iterations (0 input-starved)
perf: consumer/poll=4.43s (44%) consumer/parse=0.48s (5%) producer/serialize=2.10s (21%) producer/enqueue=0.35s (4%) producer/deliver=1.51s (15%) commit=0.38s (4%)
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
unaccounted work visible. Time spent on another thread belongs in a
`PhaseStats` of its own, which `report()` logs as one `perf <label>:` line
(`label=`), as the node harness does for reading. Each line is a share of the
same interval for a different thread, so lines are not to be added up. Track `starved_iterations` for iterations that were
blocked waiting on input: `consumer/poll` blocks until the batch fills or the timeout
expires, so on an under-fed node it approaches 100% and nothing else on the
line means anything.

### Prometheus metrics

`tkati_core.metrics` exposes a `LoopStats` as Prometheus counters: the same
numbers as the log line, but monotonic, so they don't reset at each report.

```python
from tkati_core import MetricsSettings, start_metrics_server

stats = LoopStats(phases=PHASES)
start_metrics_server(MetricsSettings(), stats)  # :8000/metrics, daemon thread
```

| Metric | Labels | Meaning |
|---|---|---|
| `tkati_phase_seconds_total` | `phase` | wall clock spent in each phase |
| `tkati_wall_seconds_total` | — | wall clock since the `LoopStats` was created, the 100% denominator |
| `tkati_rows_in_total` / `tkati_rows_out_total` | — | rows read / written, counted when their batch is committed |
| `tkati_iterations_total` | — | loop iterations |
| `tkati_starved_iterations_total` | — | iterations that waited on input |

There is no node label: each node serves its own `/metrics`, so the scrape
target's `job`/`instance` already says which node a series came from. `phase`
is the phase name exactly as it appears in the log line. Every declared phase
is exported from the first scrape, at 0 if it hasn't run yet. A node exports
its loop's stats only: `perf read:`'s phases are in the log alone.

The log line's "% of the interval", and its unaccounted remainder, in PromQL:

```promql
rate(tkati_phase_seconds_total[1m])
  / ignoring(phase) group_left rate(tkati_wall_seconds_total[1m])

1 - sum without (phase) (rate(tkati_phase_seconds_total[1m]))
  / rate(tkati_wall_seconds_total[1m])
```

Wall clock is its own metric rather than a `phase="total"` series, because
`sum without (phase)` over phases would otherwise count it twice. Throughput is
`rate(tkati_rows_in_total[1m])`. The starved share is
`rate(tkati_starved_iterations_total[1m]) / rate(tkati_iterations_total[1m])`.

Values are read from `LoopStats.totals()` when Prometheus scrapes, so exporting
adds nothing to the loop. `MetricsSettings` (`enabled`, `port`, `addr`) is meant
to be embedded as a `metrics` section in a node's settings. It is on by default,
and `enabled = false` makes `start_metrics_server` a no-op. The collector,
`LoopStatsCollector`, can also be registered on your own `CollectorRegistry`
if you serve metrics yourself.

### `tkati_core.settings` — generic node settings aliases

`tkati_core.settings` defines `InputSettings`/`OutputSettings` (discriminated unions
over every input/output kind `tkati-core` implements), and `NodeSettings`, the
`input`/`output`/`dlq`/`metrics` sections `SyncNode.from_settings` reads. Use those aliases with the
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


settings = (
    AppSettings()
)  # settings.input.connection.broker, settings.input.topic.name, ...
consumer = KafkaConsumer.from_input_settings(settings.input)

# Read a batch
batch = consumer.read_arrow(
    timeout=settings.input.consumer.batch_timeout_sec,
    num_messages=settings.input.consumer.batch_size,
)
if batch is not None:
    process(batch.data)
    consumer.commit(batch)
```

The factory method sets `enable.auto.commit=False`: each batch's offsets are
committed explicitly with `consumer.commit(batch)`.

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
