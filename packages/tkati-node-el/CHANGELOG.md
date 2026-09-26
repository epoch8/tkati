# 0.5.2

* **Fixed at-least-once delivery for a Kafka output.** The node committed the
  input offset as soon as the batch was enqueued, so a crash could lose rows
  that had not reached the broker. It now waits for delivery (`flush`) before
  committing. A ClickHouse output is unaffected, since its inserts were already
  synchronous
* The node logs where its wall clock went every 10 seconds, in the same format
  as `tkati-node-dedup`: `consumer/poll`, `consumer/parse`,
  `producer/serialize`, `producer/enqueue`, `producer/deliver` and `commit`.
  The per-batch `Produced N rows` line moved to `DEBUG`. See the README
* **Now listens on port 8000 by default**, serving the perf report's numbers
  as Prometheus metrics at `/metrics`. Disable with `[metrics] enabled = false`
  or `METRICS__ENABLED=false`
* The output producer is closed on shutdown. Before, only the DLQ producer
  was

# 0.3.0

* Initial implementation of `tkati-node-el`: a generic extract/load node that reads
  batches from a configurable input and writes them to a configurable output, picking
  input/output kind from each section's `type` field (`"kafka"` for input;
  `"kafka"`/`"clickhouse"` for output and DLQ)
* At-least-once delivery: input offsets are committed only after a successful write
* Recursive DLQ fallback for the `clickhouse` output kind — a failing batch is split
  and retried down to individual rows, with unwritable rows sent to the DLQ sink
