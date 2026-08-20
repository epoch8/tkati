# WIP 0.3.0

* Initial implementation of `tkati-node-dedup`: a Kafka-to-Kafka node that
  deduplicates events by a configurable field within a rolling
  processing-time window (e.g. "dedup by uid on a 3 hour window")
* Dedup state is tracked in an embedded RocksDB store, partitioned into
  hourly on-disk buckets that are automatically cleaned up once they age out
  of the window
* At-least-once delivery: a key is only marked seen after a confirmed,
  flushed produce, and the input offset is only committed after that — on any
  crash or dedup-store failure the node prefers forwarding a possible
  duplicate over dropping a real event
