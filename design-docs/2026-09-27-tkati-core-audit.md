# tkati-core audit: correctness and efficiency

|              |                                                                                             |
| ------------ | ------------------------------------------------------------------------------------------- |
| **Date**     | 2026-09-27                                                                                  |
| **Revision** | 0.8.0, commit `fe91ce7`                                                                     |
| **Scope**    | all of `packages/tkati-core`: the Rust extension, the Kafka and ClickHouse wrappers, the node harness, stats and metrics |
| **Tests at audit time** | 162 Python tests and 11 Rust unit tests, all passing                             |

## Summary

The fast path is in good shape. Heavy work runs with the GIL released.
Consumed payloads reach pyarrow's JSON reader without a copy, through the
buffer protocol on `RawBatch`. Encoding to JSON is spread over rayon.
Delivery tracking waits on a condvar rather than polling. Encoding one int64
column runs at about 40 ms per 1M rows on 14 cores.

The problems are in the failure paths. One malformed message halts a node
for good (F1). A failed write to a Kafka DLQ loses rows without an error
(F2). A ClickHouse outage empties the whole batch into the DLQ (F3). Commit
failures are invisible (F4). Fix F1 and F2 first: both are small changes.

None of the efficiency findings is urgent. Together they are worth
single-digit percentages.

## Findings

Severity:

- **Critical:** halts a pipeline or loses data.
- **High:** puts data in the wrong place.
- **Medium:** hides failures or weakens guarantees.
- **Low:** edge cases.

| ID  | Severity | Finding                                                       | Status |
| --- | -------- | ------------------------------------------------------------- | ------ |
| F1  | Critical | One bad message stops a `consume_arrow()` node for good        | Open   |
| F2  | Critical | A failed write to a Kafka DLQ loses rows without an error      | Open   |
| F3  | High     | A ClickHouse outage sends the whole batch to the DLQ           | Open   |
| F4  | Medium   | Commit errors are never reported                               | Open   |
| F5  | Medium   | Kafka settings can't be configured; producer defaults can reorder retried messages | Open |
| F6  | Low      | No handling of revoked partitions: more duplicates after a rebalance | Open |
| F7  | Low      | Offset ranges are wrong if a partition is re-assigned mid-poll | Open   |
| F8  | Low      | Large tables in the `arrow-batch` format fail at enqueue       | Open   |
| F9  | Low      | Empty payloads are dropped with only a warning                 | Open   |
| F10 | Low      | `poll_batch(timeout=0)` doesn't poll                           | Open   |
| F11 | Low      | The metrics server can only be started once per process        | Open   |

| ID  | Efficiency finding                                              | Status |
| --- | --------------------------------------------------------------- | ------ |
| E1  | Serial copy after the parallel encode, and over-sized buffers   | Open   |
| E2  | Timestamp columns encode about 1.8× slower than int64           | Open   |
| E3  | The `arrow-batch` payload is copied twice                       | Open   |
| E4  | One FFI call per consumed or produced message                   | Open   |
| E5  | Throughput is left on the table by configuration                | Open   |

---

### F1. One bad message stops a `consume_arrow()` node for good

**Severity:** Critical
**Where:** `KafkaConsumer.read_arrow`, `parse_ndjson`
(`tkati_core/kafka/consumer.py`)

**Problem.** `read_arrow` parses the whole batch in one call. One malformed
payload makes pyarrow raise for the whole batch. A message with no value (a
tombstone) raises too, on purpose (`if batch.tombstones: raise ValueError`).

**Impact.** The exception propagates out of the node's loop, and
`_NodeBase.__exit__` rewinds the batch. After a restart the node reads the
same batch and fails again. It can't make progress until someone removes the
message by hand. tkati-node-dedup reads with `consume_arrow()`, so it is
exposed. `read_pylist` already skips bad messages, but a node that uses it
loses the Arrow fast path.

**Evidence.** Two payloads, `{"a":1}` and `not json`, parsed with
`parse_ndjson(RawBatch.from_payloads(...))`, raise
`ArrowInvalid: JSON parse error: Invalid value. in row 1`.

**Recommendation.** When the whole-batch parse fails, parse the halves of
the batch recursively until the bad messages are isolated. Then build the
table from the rest with `pa.concat_tables`, and handle tombstones the same
way. Send the bad payloads to a DLQ, or skip them with an error log. Count
them in a `bad_messages` metric (`LoopStats`, `LoopStatsCollector`). Test
with a malformed message and a tombstone in the middle of a batch.

### F2. A failed write to a Kafka DLQ loses rows without an error

**Severity:** Critical
**Where:** `ClickhouseProducer.produce_arrow`
(`tkati_core/clickhouse/producer.py`); `Deliveries::wait_all`,
`KafkaProducer::flush_step` (`src/kafka.rs`)

**Problem.** `ClickhouseProducer.produce_arrow` sends the rows ClickHouse
rejects with `dlq_producer.produce_arrow(table)`, without a tag, then calls
`self._dlq_producer.flush()`. For a Kafka DLQ, `flush()` ends in
`Deliveries::wait_all`, which returns once nothing is in flight. A failed
message counts as settled just like a delivered one, so `flush()` returns
normally.

**Impact.** If a DLQ write fails, the rows are in neither ClickHouse nor the
DLQ, and the batch is committed. `SyncNode.done()` covers the main output by
calling `wait_delivered(tag)` after `flush()`, which raises `DeliveryError`.
The DLQ path has no tag, so it has nothing to call `wait_delivered` with.

**Recommendation.** Make `flush()` raise `DeliveryError` if any message
failed since the previous flush. To do that, `Deliveries` records the first
failure among all messages, not only per tag, and `flush_step` returns it.
This also protects any other caller that relies on `flush()`. Update
`_native.pyi` and the `KafkaProducer.flush` docstring. The alternative is to
tag the DLQ produce and call `wait_delivered` on the tag, but that only fixes
this one call site.

### F3. A ClickHouse outage sends the whole batch to the DLQ

**Severity:** High
**Where:** `_insert_with_dlq_fallback`, `_insert_with_retry`
(`tkati_core/clickhouse/producer.py`)

**Problem.** `_insert_with_dlq_fallback` splits a batch after an insert fails
for any reason, including connection refused and timeouts.
`_insert_with_retry` retries every insert 3 times with a 1-second wait, at
every level of the split.

**Impact.** With the default `dlq_split_factor = 10`, a 1,000-row batch
makes about 1,100 failing insert calls (1 + 10 + 100 + 1,000). With two
1-second waits per call, that is over half an hour. After that, every row
has gone to the DLQ as a "rejected" single row, and the batch is committed.
An outage turns into a DLQ full of good data.

**Evidence.** Arithmetic on the default settings, not a measurement.

**Recommendation.** Split only for errors caused by the data: ClickHouse's
type and parse errors, which can be told apart by their server error code.
For anything else, raise so the batch is rewound. Limit the retries to
errors that aren't data errors. Test with a connection error and a type
error.

### F4. Commit errors are never reported

**Severity:** Medium
**Where:** `KafkaConsumer::commit`, `StderrContext` (`src/kafka.rs`);
`done()` docstrings (`tkati_core/node.py`)

**Problem.** Commits are sent with `CommitMode::Async`. `StderrContext`
doesn't override `ConsumerContext::commit_callback`, and in rdkafka 0.39 the
default does nothing. So a failed commit is dropped without a trace.
Examples are `REBALANCE_IN_PROGRESS`, `ILLEGAL_GENERATION`, or a consumer
group the client has no ACL for.

**Impact.** `done()` returns once the commit has been *sent*, not once it has
been made. That contradicts the docstrings (the `node.py` module docstring
and `SyncNode.done`: "returns once the commit is made"). If commits keep
failing, consumer lag grows with no log line and no metric. For
tkati-node-dedup the effect is otherwise limited: the output is delivered
before the commit, so a batch re-read after a failed commit is correctly
dropped as duplicates. Shutdown doesn't lose pending commits: dropping a
`BaseConsumer` with a group closes it, and librdkafka waits for outstanding
commits during the close.

**Recommendation.** Give the consumer its own context with a
`commit_callback`. It logs failures to stderr, as the other callbacks do,
counts them, and keeps the first error so the next `NativeConsumer.commit`
raises it. Export the count as a metric, and fix the `done()` docstrings.

### F5. Kafka settings can't be configured; producer defaults can reorder retried messages

**Severity:** Medium
**Where:** `KafkaConsumer.from_input_settings`
(`tkati_core/kafka/consumer.py`), `KafkaProducer.from_topic_settings`
(`tkati_core/kafka/producer.py`), `tkati_core/kafka/settings.py`

**Problem.** Clients are built from `bootstrap.servers` alone. The consumer
also sets `group.id`, `auto.offset.reset` and `enable.auto.commit`. Nothing
else can be set from settings: no SASL, compression, `client.id`, `acks` or
idempotence.

**Impact.** The producer runs with librdkafka's defaults:
`enable.idempotence=false`, unlimited retries, and up to 1,000,000 in-flight
requests per connection. With those, a retried send can land after a later
one, and can be duplicated. That breaks per-key order within a partition,
which is what `key_column` is for.

**Recommendation.** Add `config: dict[str, str | bool] = {}` to
`KafkaConnectionSettings`, merged last into the client config. Default the
producer to `enable.idempotence=true`, which makes librdkafka limit in-flight
requests and keep order across retries. Also default to
`compression.type=lz4` or `zstd`; the crate already links zstd. TLS is a
separate change: it is excluded in `Cargo.toml` for build reasons that are
documented there.

### F6. No handling of revoked partitions: more duplicates after a rebalance

**Severity:** Low
**Where:** `KafkaConsumer::assigned` (`src/kafka.rs`)

A batch can be finished but not yet committed on a partition that was just
revoked. `assigned` filters that partition out, so the batch is never
committed and the partition's new owner reads it again. That is still
at-least-once, but duplicates grow with `[pipeline] max_in_flight`. A revoke
callback that settles finished batches first would reduce them.

### F7. Offset ranges are wrong if a partition is re-assigned mid-poll

**Severity:** Low
**Where:** `record_offset` (`src/kafka.rs`)

`record_offset` assumes offsets only grow within a partition during one
batch. If the partition is revoked and re-assigned during a poll, the fetch
restarts from the committed offset and offsets go backwards. The recorded
range is then wrong: the commit can move backwards, and a rewind can skip
messages. Taking the minimum for the start and the maximum for the end fixes
it at no cost.

### F8. Large tables in the `arrow-batch` format fail at enqueue

**Severity:** Low
**Where:** `KafkaProducer.produce_arrow` (`tkati_core/kafka/producer.py`)

The `arrow-batch` format sends the whole table as one message.
`message.max.bytes` defaults to 1 MB, so a large table fails at enqueue.
That fails the batch, which is rewound and fails again after a restart, the
same loop as F1. At least check the size first and raise a clear error.

### F9. Empty payloads are dropped with only a warning

**Severity:** Low
**Where:** `Payloads::push` (`src/payloads.rs`), `read_arrow`

A message with an empty but non-null value becomes an empty line in the
NDJSON buffer, and pyarrow skips it. The only sign is a row-count warning.
Probe: 3 messages, one of them empty, gave 2 rows. Decide whether this
counts as a bad message under F1.

### F10. `poll_batch(timeout=0)` doesn't poll

**Severity:** Low
**Where:** `NativeConsumer::poll_batch` (`src/lib.rs`)

The deadline is checked before the first poll, so `timeout=0` returns an
empty batch without polling at all. Poll at least once.

### F11. The metrics server can only be started once per process

**Severity:** Low
**Where:** `start_metrics_server` (`tkati_core/metrics.py`)

It registers its collector on the global `REGISTRY` and never stops the HTTP
server. Entering a second node in the same process fails on duplicate
registration and on the port. This mostly matters for tests.

---

### E1. Serial copy after the parallel encode, and over-sized buffers

**Where:** `encode_rows`, `encode_range`, `Messages::append`
(`src/encode.rs`)

Chunks are encoded in parallel, then copied into one buffer on one thread.
Each chunk also reserves `rows × 64 × columns` bytes up front, which for wide
tables is hundreds of MB of virtual memory. Keeping the chunks as they are,
indexed by chunk and offset, removes the copy. Sizing the reservation from a
sample of rows would fix the over-reservation.

### E2. Timestamp columns encode about 1.8× slower than int64

**Where:** `TimestampEncoder::encode` (`src/encode.rs`)

A timestamp column takes 72 ms per 1M rows, against 40 ms for int64, on 14
cores. Each value goes through chrono's format strings and is converted to
the time zone twice. Writing the digits directly would close most of the
gap.

### E3. The `arrow-batch` payload is copied twice

**Where:** `KafkaProducer.produce_arrow`

The payload is copied by `to_pybytes()`, and again into a `Vec<u8>` by
`EncodedBatch.from_payloads`.

### E4. One FFI call per consumed or produced message

**Where:** `KafkaConsumer::poll_step`, `KafkaProducer::enqueue`
(`src/kafka.rs`)

Consuming calls `poll` once per message, and producing calls `send` once per
message, each about 1 µs. librdkafka has batch APIs for both, but
rust-rdkafka doesn't expose them. Only worth doing if a profile shows it.

### E5. Throughput is left on the table by configuration

The larger throughput gains are likely in producer compression and
`linger.ms` (needs F5), and in running nodes on `PipelinedNode` rather than
`SyncNode` where they allow it.

Measure every efficiency change before and after with
`benchmarks/bench_kafka_json.py` and `benchmarks/bench_node_pipeline.py`.

## Method

- **Code reading, at `fe91ce7`:** F2–F8, F10 and F11. For F4, rdkafka 0.39's
  default `commit_callback` (`src/consumer/mod.rs`) is an empty function. The
  `Drop` implementations of `BaseConsumer` and `BaseProducer` were read to
  confirm shutdown behaviour: the consumer closes and waits, and the producer
  purges whatever is still queued.
- **Probes against the built extension:** F1, F9 and E2. They ran
  `parse_ndjson` on `RawBatch.from_payloads([...])`, and timed `encode_arrow`
  on a 1M-row table with one `timestamp[ms]` or one `int64` column.
- **Arithmetic on the default settings:** F3.
