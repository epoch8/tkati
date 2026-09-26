---
status: IMPLEMENTED
---

# Align tkati-node-el's loop with tkati-node-dedup

## Context

Over releases 0.4.3–0.5.1, `tkati-node-dedup`'s loop
(`packages/tkati-node-dedup/src/tkati_node_dedup/main.py`) gained observability:

- a `LoopStats` with a declared phase tuple, `_PHASES`, spliced from
  `CONSUMER_PHASES` and `PRODUCER_PHASES`;
- `stats=` handed to `Consumer.read_arrow`, `Producer.produce_arrow` and
  `Producer.flush`, so tkati-core times the `consumer/*` and `producer/*`
  phases;
- counts of iterations, starved iterations (no batch, or a short one),
  `rows_in` and `rows_out`;
- a perf log line every 10s (`LoopStats.report_if_due`);
- Prometheus counters on `:8000/metrics` (`MetricsSettings`,
  `start_metrics_server`).

`tkati-node-el`'s loop (`packages/tkati-node-el/src/tkati_node_el/main.py`)
was still the 0.3.0 original. It had none of the above and logged one `INFO`
line per batch.

The comparison also turned up a correctness gap. node-el called
`consumer.commit()` right after `producer.produce_arrow()` and never called
`flush()`. `KafkaProducer.produce_arrow` only enqueues to librdkafka, so with a
Kafka output a crash could lose rows whose offsets were already committed. That
contradicts the at-least-once guarantee in node-el's README. node-dedup already
flushed before committing. `ClickhouseProducer` inserts synchronously, so a
ClickHouse output was not affected.

## Goal

Done when:

- node-el logs the same perf report as node-dedup, with node-el's phases:
  `consumer/poll`, `consumer/parse`, `producer/serialize`, `producer/enqueue`,
  `producer/deliver`, `commit`.
- node-el serves those numbers on `/metrics`, on by default and configured by a
  `[metrics]` section.
- node-el commits an input offset only after its batch is confirmed delivered,
  whatever the output kind.
- `run_one_iteration` takes an optional `stats`, with the same signature shape
  as node-dedup's. Tests check that `stats` reaches the consumer and producer,
  that a failed flush doesn't commit, and what gets counted.

Non-goals:

- Pulling a shared loop runner out into tkati-core. There are only two nodes,
  and their loops differ in the middle (lookup/write, store cleanup).
- Any change to node-dedup's loop. The one exception is the shutdown fix
  below, which both nodes needed.
- A node-el metric of its own. node-el never drops rows, so there is no analogue
  of `tkati_node_dedup_dropped_rows_total`.

## Approach

Port node-dedup's loop structure to node-el as it stands, without the
dedup-specific parts, keeping its ordering and comments so the two files read as
siblings. The phase tuple is `(*CONSUMER_PHASES, *PRODUCER_PHASES, "commit")`,
the example tkati-core's README already gives for an extract/load node.

The flush goes in because it is what the at-least-once promise requires. It
also makes `producer/deliver` mean something for a Kafka output. The cost is
that each iteration waits for broker acks. Throughput could only be recovered
by committing offsets asynchronously once delivery callbacks fire, which is a
larger change and not needed yet.

## Implementation notes (as built)

Neither node closed its output producer on shutdown, only the DLQ producer. Both
`main()`s now call `producer.close()` in their `finally`, after the consumer and
before the DLQ producer, because the output can still route rows to the DLQ.
With a flush on every iteration nothing is normally left queued, so this is
about releasing the client (librdkafka handle, ClickHouse connection) rather
than about losing data.

## Verification

- `uv run ruff check` and `uv run ty check` are clean on both
  `packages/tkati-node-el` and `packages/tkati-node-dedup`.
- `uv run pytest packages/tkati-node-el`: all 7 pass. That is the 4 new
  broker-free tests (stats handed down, no commit after a failed flush, row and
  starved counting, metrics on by default) plus the existing ClickHouse and
  Kafka round-trip integration tests against local brokers.
