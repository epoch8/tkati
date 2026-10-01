# WIP 0.8.4

* DLQ sinks now receive rejected rows using the DLQ schema
  `producer`/`data`/`err_message`/`time`; `producer` contains the source
  ClickHouse URL, database, and table as JSON

# 0.8.3

* `[input.config]` and `[output.config]` pass arbitrary librdkafka properties
  through to the consumer and producer — compression, `linger.ms`, `acks`,
  `client.id`, `SASL_PLAINTEXT`/`PLAIN` auth. Values may be strings, ints or
  bools, and librdkafka validates names and values when the client is built.
* The properties `tkati-core` sets itself (`PRODUCER_RESERVED`,
  `CONSUMER_RESERVED`) are rejected in `config` at settings-parse time instead
  of being silently overridden. `enable.auto.commit` is the one that matters:
  librdkafka accepts `true` silently, and it would turn a node's rewind-on-
  failure path into data loss.
* No defaults changed: compression is still off and `enable.idempotence` still
  false unless `config` says otherwise.

# 0.8.2

* Fix: a ClickHouse outage no longer empties a batch into the DLQ.
  `ClickhouseProducer` now classifies an insert failure by the ClickHouse error
  code rather than treating every exception alike. A parse or value error — the
  codes in the new public `CH_DATA_ERROR_CODES` — skips the retries entirely and
  goes straight to the recursive split, so isolating a bad row costs round-trips
  instead of 2 seconds of sleep per level, and the good rows around it still
  land. Anything else — connection refused, a timeout, auth, a schema error such
  as `TYPE_MISMATCH`, or no code at all — is retried 3 times and then raised, so
  the batch is rewound and re-read instead of being filed as rejected. Audit
  finding F3; see `design-docs/2026-09-28-clickhouse-error-classification.md`
* `clickhouse-connect` now requires `>=1.4.2`, for `Error.code`

# 0.8.1

* Fix: `LoopStats.rows_in` is now counted when a batch is committed, like
  `rows_out`, not when it is read. Under `PipelinedNode` a report could fall
  between the two, so the perf line's "dropped" could go negative
  (`90000 rows in, 100000 out (-10000 dropped)`). `tkati_rows_in_total` now
  counts only committed batches, so a rewound batch is no longer counted twice
* The node perf report splits its phases by thread: `perf loop:` for the loop
  thread and `perf read:` for reading. Before, with read-ahead, the two
  threads' shares overlapped on one line. New `wait/loop` phase on the read
  line: the read-ahead thread waiting for the loop to take a batch. Without
  read-ahead, `wait/input` now times the loop's own read. `DEFAULT_PHASES`,
  `SINK_PHASES` and `PIPELINED_PHASES` no longer contain `CONSUMER_PHASES`.
* **Breaking:** a node's `phases=` must not list `CONSUMER_PHASES`. Reading
  is timed in a stats object of its own, `node.read_stats`, which is logged
  with the loop's stats but not exported as metrics, so the consumer phases
  leave `/metrics`
* New `PhaseStats`: one thread's phase timings, logged as a single
  `perf <label>:` line. `LoopStats` is now a `PhaseStats` with the loop's
  counts, and takes `label=`; consumers and producers take a `PhaseStats`.
  `LoopStats.report_if_due()` returns whether it reported

# 0.8.0

* New `PipelinedNode`: `done()` returns before delivery and takes
  `after_commit=`; batches are committed in read order once delivered, with
  at most `max_in_flight` (`[pipeline]`, default 4) waiting. New
  `PIPELINED_PHASES`, and a `wait/in-flight` phase it requires.
* `tkati_core.testing`: `memory_pipelined_node`; `MemoryProducer` gains
  `deliver="manual"`, `release`, `fail`, `on_wait` and `waited_on`, and logs
  `wait:<tag>` only for waits that may block.
* Read-ahead: `SyncNode(..., read_ahead=n)` reads up to `n` batches on a
  background thread while the loop body works. `from_settings` takes it from
  the new `[pipeline]` section (`PipelineSettings`, default `read_ahead = 1`);
  direct construction defaults to 0.
* **Breaking for custom `phases=` tuples:** `wait/input` (time the loop waited
  for the reader) is now required, and is part of `DEFAULT_PHASES` and
  `SINK_PHASES`.
* `KafkaConsumer`: its batch numbering and ordering check are thread-safe. The
  native consumer lets `commit`/`rewind` run while another thread polls, and
  `close()` interrupts such a poll within one `POLL_STEP`.
* `LoopStats.record` takes the stats lock.
* **Breaking:** `Node` is renamed to `SyncNode`, with the same API. There is
  no alias.
* **Breaking for `Producer` implementations:** `produce_arrow` and
  `produce_pylist` take `tag: int | None = None`, and `wait_delivered(tag,
  timeout=None)` is a new abstract method.
* **Fixed:** a Kafka message that failed delivery (rejected by the broker, or
  given up on by librdkafka) no longer lets its batch be committed.
  `SyncNode.done()` waits on the batch's tag after the flush and raises
  `DeliveryError`, so the batch is rewound. New export: `DeliveryError`.
* **Fixed:** `KafkaProducer.flush()` no longer takes at least 100 ms. It
  waits on the delivery reports instead of librdkafka's flush, which with a
  threaded producer only returned once its whole 100 ms step had passed.
* `tkati_core._native`: `NativeProducer.enqueue(batch, tag=0)`,
  `NativeProducer.wait_delivered(tag, timeout=None)`, `DeliveryError`.

# 0.7.0

* New `Node` (`tkati_core.node`): the worker loop harness.
  * Used as `with Node.from_settings(settings) as node:` then
    `for event in node.consume_arrow():` (or `consume_pylist()`, for rows as
    dicts with undecodable messages skipped). It yields a `Batch` (`.data`,
    `.short`) for each batch read and `Idle` for an empty poll.
  * The node finishes each batch with `node.done(event, output_arrow=table)`
    (or `output_pylist=rows`). It
    sends the output, waits for delivery and commits the batch, and returns
    once the commit is made. Asking for the next event without it raises and
    rewinds the batch. An exception before `done()` rewinds the batch too.
    `break` and `KeyboardInterrupt` leave it uncommitted.
  * `phase` and `stop` are the other calls node code makes.
  * It builds and closes the consumer, output and DLQ, runs `LoopStats` and
    the metrics server, and turns SIGTERM/SIGINT into a stop after the current
    batch.
  * The producer is optional, for nodes that deliver their output themselves.
* New `NodeSettings`: the `input`/`output`/`dlq`/`metrics` sections
  `Node.from_settings` reads. `output` is optional, and `dlq` without `output`
  fails validation.
* New `tkati_core.testing`: `MemoryConsumer`, `MemoryProducer` and
  `memory_node`, for testing nodes without a broker.
* New exports: `Batch`, `Idle`, `Event`, `Node`, `NodeSettings`,
  `DEFAULT_PHASES`, `SINK_PHASES`.

# 0.6.0

* **Breaking:** explicit per-batch commit.
  * `read_arrow` / `read_pylist` return a `ConsumedBatch` (exported from
    `tkati_core`) instead of a bare `pa.Table` / `list[dict]`. The table or
    list is its `.data`.
  * `commit()` becomes `commit(batch)`. It commits exactly that batch's offsets,
    not the consumer's whole position. Before, every message polled so far was
    committed, including batches read but not yet processed.
  * New `rewind(batch)` marks a batch as failed. The consumer seeks back to
    where the batch started, so it (and everything read after it) is read
    again.
  * `commit` and `rewind` must be called for the oldest outstanding batch, in
    read order. Anything else raises `ValueError`.
  * Offsets for partitions no longer assigned after a rebalance are skipped,
    as `commit()` already skipped them.
* `tkati_core._native`: new opaque `BatchOffsets`; `RawBatch.offsets`;
  `NativeConsumer.commit(offsets)` and `rewind(offsets)`.

# 0.5.1

* Prometheus metrics no longer carry a `node` label. Each node serves its own
  `/metrics`, so the scrape target's `job`/`instance` identifies it. Queries
  that grouped with `sum by (node)` should use `sum without (phase)` instead.
* `LoopStats.name` is removed. Callers drop the `name=` argument, and the perf
  log lines no longer start with it (`perf over 10s: ...`, `perf: ...`).

# 0.5.0

* `KafkaConsumer` and `KafkaProducer` run on a native (Rust) extension,
  `tkati_core._native`, with librdkafka linked in statically. Their
  constructors, `from_*` classmethods, attributes, methods and `stats` phases
  are unchanged.
  * `produce_arrow` with `format="json"` encodes rows straight from the Arrow
    columns, in parallel across all cores, instead of via `to_pylist()` +
    orjson. This makes it about 30x faster, e.g. 7.4 s → 0.24 s per 1M rows of
    tkati-node-el's schema. Output is byte-identical for strings, integers,
    booleans, nulls, nested lists/structs and timestamps; float formatting
    differs (see below).
  * `read_arrow` builds its batch buffer natively as messages arrive and hands
    it to pyarrow's JSON reader zero-copy, split into blocks so that every core
    parses a share. It is 1.7–2.8x faster, e.g. 0.43 s → 0.23 s per 1M rows,
    and parsing semantics are unchanged.
  * Polling and enqueueing loop natively rather than calling into
    confluent-kafka once per message.
  * `read_pylist` and `produce_pylist` still use orjson. Building or reading
    Python dicts needs the GIL either way, and a native parse benchmarked
    slower.
  * The JSON encoder's parallelism follows `RAYON_NUM_THREADS` (default: all
    available cores).
* **Breaking:**
  * Kafka errors from `commit`, construction etc. raise
    `tkati_core._native.KafkaError`, not `confluent_kafka.KafkaException`.
  * `KafkaConsumer.consumer` / `KafkaProducer.producer` are now the native
    client objects, not confluent-kafka's.
  * No TLS for now: librdkafka is built without OpenSSL, so a
    `kafka_config` with `security.protocol` `SSL` or `SASL_SSL` fails at
    construction. tkati's own settings never configure TLS. See the rdkafka
    line in `Cargo.toml` to re-enable it.
  * Building tkati-core from source needs a Rust toolchain. Wheels are
    published for manylinux x86_64 and aarch64 (abi3, CPython ≥ 3.13).
* Behaviour changes:
  * `produce_*` no longer raises `BufferError` when librdkafka's local queue
    is full: it waits for the queue to drain and retries.
  * A tombstone (message with no value) in a `read_arrow` batch raises a
    `ValueError` naming how many there were, instead of a `TypeError`.
    `read_pylist` still skips and logs it.
  * `produce_arrow` JSON: floats are written in the shortest round-trip form
    with a `1.0e16`-style exponent (orjson wrote `1e+16`), and float32 values
    are no longer widened to float64 first. Decimal and binary columns are
    encoded, as a JSON number and a hex string respectively, where orjson
    raised `TypeError`.
* librdkafka logs still go to stderr, as with confluent-kafka.
* confluent-kafka remains a dependency, for `tkati_core.kafka.testing`'s
  `AdminClient`.

# 0.4.5

* `Producer.produce_arrow`, `produce_pylist` and `flush` take an optional
  `stats: LoopStats` and split their time into `producer/serialize`,
  `producer/enqueue` and `producer/deliver`. `KafkaProducer` now encodes a
  batch fully before enqueueing it. `ClickhouseProducer` records everything as
  `producer/deliver`
* Add `PRODUCER_PHASES`, re-exported from `tkati_core`
* `KafkaConsumer.read_pylist` takes `stats`, split like `read_arrow`
* `read_pylist` is now an abstract method on the base `Consumer`, so it can be
  called on whatever `build_consumer` returns. **Breaking** for any
  out-of-tree `Consumer` subclass, which must now implement it
* Add `tkati_core.metrics`: `LoopStatsCollector` exports a `LoopStats` as
  Prometheus counters (`tkati_phase_seconds_total{node,phase}`,
  `tkati_wall_seconds_total`, rows in/out, iterations, starved iterations), and
  `start_metrics_server(MetricsSettings, stats)` serves them. New dependency:
  `prometheus-client`
* `LoopStats.totals()` returns everything counted since creation, across
  reports. `report()`/`reset()` still restart the interval as before
* **Breaking for log parsers:** `CONSUMER_PHASES` is now
  `("consumer/poll", "consumer/parse")`. Phases timed inside core are prefixed
  with their component

# 0.4.4

* `Consumer.read_arrow` takes an optional `stats: LoopStats` and splits its own
  wall clock into `poll` (fetching from the broker) and `parse` (decoding into
  Arrow), so callers can tell broker latency apart from JSON cost
* Add `CONSUMER_PHASES` (`("poll", "parse")`, re-exported from `tkati_core`) for
  nodes to splice into their `LoopStats` phase tuple
* The consumer's per-batch log lines moved from `info` to `debug` — at
  production throughput they were tens of lines a second and buried the periodic
  perf report. Row-count mismatches and parse failures are unaffected

# 0.4.3

* Add `LoopStats` (`tkati_core.stats`, re-exported from `tkati_core`): per-phase
  wall-clock accounting for a node's main loop, logged on an interval (10s by
  default) as seconds and percent. Phase names, log prefix and cadence are
  constructor arguments so each node can describe its own pipeline. Moved here
  from `tkati-node-dedup`, which had it inline

# 0.3.0

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
* Add `InputSettings`/`OutputSettings` discriminated unions to `tkati_core.settings`,
  plus `build_consumer` (in `tkati_core.consumer`) and `build_producer` (in
  `tkati_core.producer`) factories, so a generic node picks its input/output
  implementation from a settings object's `type` field without hardcoding a
  concrete class
* `Consumer`, `Producer`, `InputSettings`, `OutputSettings`, `build_consumer`, and
  `build_producer` are now re-exported from the top-level `tkati_core` package
* **Breaking:** `Consumer.read_arrow`/`KafkaConsumer.read_arrow`/`read_pylist` params
  renamed: `aggregation_interval_seconds` → `timeout`, `max_events_to_aggregate` →
  `num_messages`
* **Breaking:** `ClickHouseOutputSettings` gains a `dlq_split_factor: int = 10` field;
  `build_producer` no longer takes a `split_factor` kwarg — it derives the value from
  `settings.dlq_split_factor` when `settings` is a `ClickHouseOutputSettings`

# 0.2.0

* Initial implementation of ClickhouseProducer

# 0.1.0

* Initial implementation of KafkaConsumer and KafkaProducer
