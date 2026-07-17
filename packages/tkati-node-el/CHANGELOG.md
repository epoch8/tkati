# WIP 0.3.0

* Initial implementation of `tkati-node-el`: a generic extract/load node that reads
  batches from a configurable input and writes them to a configurable output, picking
  input/output kind from each section's `type` field (`"kafka"` for input;
  `"kafka"`/`"clickhouse"` for output and DLQ)
* At-least-once delivery: input offsets are committed only after a successful write
* Recursive DLQ fallback for the `clickhouse` output kind — a failing batch is split
  and retried down to individual rows, with unwritable rows sent to the DLQ sink
