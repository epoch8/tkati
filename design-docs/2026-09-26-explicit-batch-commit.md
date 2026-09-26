---
status: IMPLEMENTED
---

# Explicit per-batch commit and rewind

## Context

Until 0.5.x, `Consumer.commit()` (`packages/tkati-core/tkati_core/consumer.py`)
took no arguments. The native `KafkaConsumer::commit`
(`packages/tkati-core/src/kafka.rs`) called
`commit_consumer_state(CommitMode::Async)`, which commits librdkafka's current
position: every message polled so far.

That equals "the batch just processed" only because both nodes,
`tkati_node_el/main.py` and `tkati_node_dedup/main.py`, run one batch at a
time: read, produce, flush, commit. A loop that reads batch N+1 before
committing batch N would find that `commit()` also covers N+1. A crash before
N+1 is delivered would then lose it. Pipelining the loop needs a commit that
names the work that finished.

There was also no way for a node to say that a batch failed. An exception just
propagated and stopped the node, and the consumer's position kept whatever had
been polled.

## Goal

Done when:

- `read_arrow` / `read_pylist` return a batch object that can be handed back to
  the consumer, and transforming its data doesn't affect what gets committed.
- `consumer.commit(batch)` commits exactly that batch's offsets, whatever else
  has been read since.
- `consumer.rewind(batch)` makes the batch, and everything read after it, be
  read again.
- Committing or rewinding out of read order raises instead of silently
  skipping unfinished work.
- Both nodes commit the batch they read and rewind it on failure.
- Tests against Redpanda show all of the above.

Non-goals:

- Pipelining itself. The node loops stay sequential.
- Tracking completed batches and committing the longest finished prefix. With
  strict ordering, the caller must finish batches in order. A prefix tracker is
  the natural next step once batches can complete out of order.
- Retrying after a rewind. The nodes still re-raise and stop, as before.
- Sync commits. The commit stays async, as before.

## Approach

The offsets are recorded where the messages are, in the native poll loop. For
each message it keeps the first offset and last offset + 1 per partition.
They travel with the batch as an opaque `BatchOffsets` object, which Python
never inspects and only hands back to `NativeConsumer.commit` / `rewind`.
The map is keyed by partition only, because a consumer subscribes to exactly
one topic.

`read_*` wraps the parsed data and those offsets in a `ConsumedBatch[T]`
(`data`, `offsets`, `seq`). The offsets sit beside the data, not inside it
(for example as Arrow schema metadata), so tkati-node-dedup can filter
`batch.data` and still commit the batch as read. The breaking change to the
return type is the cost of that.

`commit` commits each partition's next offset through an explicit
`TopicPartitionList`. `rewind` seeks each partition back to its first offset
with `seek_partitions`. The seek waits for the fetcher to move, so messages
already fetched from the old position are never handed out. Both skip
partitions no longer in `assignment()`: after a rebalance, those belong to
another group member. The old `commit_consumer_state` covered only the
current assignment too.

Ordering is enforced in the Python `KafkaConsumer`, which numbers batches as it
reads them (`seq`). `commit` and `rewind` both require the oldest outstanding
batch, one neither committed nor rewound.
- `commit` advances to the next batch.
- `rewind` marks every batch read so far as resolved. Their messages lie after
  the seek point, so they will come back, and committing the stale batch objects
  would skip past that re-read.

## Implementation notes (as built)

- `kafka.rs`: `Offsets = BTreeMap<i32, (i64, i64)>` and `record_offset`, called
  in `poll_step` for each `Ok` message.
  - `KafkaConsumer` keeps its `topic`, and `assigned()` builds the list,
    filtered by assignment, for `commit` / `rewind`.
  - An empty list skips the call, because librdkafka rejects one.
  - The seek waits up to `SEEK_TIMEOUT` (10s).
- `lib.rs`:
  - The `BatchOffsets` pyclass has `BatchOffsets()`, which builds an empty
    instance for test mocks, and a `__repr__`. Nothing else is exposed.
  - `RawBatch.offsets`.
  - `NativeConsumer.commit(offsets)` / `rewind(offsets)`, with the GIL
    released.
- `tkati_core/consumer.py`: `ConsumedBatch[T]`, and abstract `commit(batch)` /
  `rewind(batch)` carrying the ordering contract.
  `tkati_core/kafka/consumer.py`: the `seq` counters and `_check_oldest`.
- `read_pylist` still returns `None` when every message in a batch fails to
  parse. Such a batch gets no `seq`, and its offsets are committed once a later
  batch from the same partitions is.
- The nodes wrap everything between read and commit in
  `try` / `except Exception: consumer.rewind(batch); raise`. They catch
  `Exception`, not `BaseException`, so Ctrl-C doesn't wait on a seek during
  shutdown. tkati-node-dedup now counts `rows_out` after the batch succeeds,
  like tkati-node-el.
- Released as 0.6.0, a breaking change for tkati-core callers. Migration
  notes are in `MIGRATION.md`.

## Verification

- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and
  `cargo test` pass. `record_offset` has a unit test.
- `uv run ruff check packages/` and `uv run ty check packages/` are clean.
- `uv run pytest packages/tkati-core packages/tkati-node-el packages/tkati-node-dedup`
  passes against Redpanda and ClickHouse. The new tests in
  `packages/tkati-core/tests/test_consumer.py` cover:
  - committing b1 while b2 is also read, which commits b1's end only;
  - a commit covering all partitions of a three-partition topic;
  - a rewound batch being read again with nothing committed;
  - a rewind invalidating a later batch, whose `commit` / `rewind` then raise;
  - out-of-order and repeated commits raising.
- The node tests assert that `commit` gets the batch as read, and that a failed
  flush rewinds it instead.
