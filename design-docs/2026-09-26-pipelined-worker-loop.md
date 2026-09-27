---
status: IMPLEMENTED
---

# Pipelined worker loop

## Context

The worker loop harness (`design-docs/2026-09-26-worker-loop-harness.md`,
0.7.0) runs each node's loop in `tkati_core.Node`
(`packages/tkati-core/tkati_core/node.py`). A node reads with
`for event in node.consume_arrow():` (or `consume_pylist()`) and finishes
each batch with `node.done(event, output_arrow=...)`. `done()` is
synchronous:

1. it sends the output (`Producer.produce_arrow` / `produce_pylist`);
2. it waits for delivery (`Producer.flush`);
3. it commits the batch (`Consumer.commit`);
4. it returns.

Code after `done()` runs after the commit, and tkati-node-dedup relies on
that. It marks keys seen and counts dropped rows on the lines after `done()`
(`packages/tkati-node-dedup/src/tkati_node_dedup/main.py`).

Everything in the loop runs one step after another. An iteration waits for the
poll, parses, runs the node's logic, serializes and enqueues, waits for broker
acks, and commits. Only then does the next poll start. The poll wait and the
broker round trip both block the loop, and the CPU has nothing to do during
them. `design-docs/2026-09-26-node-el-loop-stats.md` accepted this cost when
it made node-el flush before committing. It named asynchronous commit from
delivery callbacks as the fix, and deferred it as "a larger change".

Some of the groundwork is in place:

- **Per-batch commit.** `design-docs/2026-09-26-explicit-batch-commit.md`
  (0.6.0) made `Consumer.commit(batch)` commit exactly that batch's offsets,
  and added `rewind(batch)`. Both must be called in read order. So committing
  batch N no longer covers a batch N+1 that has already been read. That doc
  named tracking finished batches, to commit the longest finished prefix, as
  "the natural next step once batches can complete out of order".
- **An explicit end of each batch.** The harness's `done()` states where a
  batch ends and ties each output to the input batch it came from. A pipelined
  harness needs exactly that link. The harness doc's section "Later:
  asynchronous `done()`" anticipates this change: `done()` returns before
  delivery, and the work after it moves into callbacks.

Two gaps remain in the layers underneath:

- **The producer can't tell which batch a delivery belongs to.**
  `StderrContext::delivery` in `packages/tkati-core/src/kafka.rs` discards
  every delivery report, and `flush()` only waits until nothing is in flight.
  There is nothing to track per-batch delivery with.
- **Delivery failures go unnoticed.** For the same reason, a message
  librdkafka gives up on (`message.timeout.ms`) lets `flush()` return as if it
  had been delivered, and `done()` commits the batch. So even today's
  synchronous loop keeps at-least-once for a Kafka output only while no
  delivery fails. `ClickhouseProducer` raises on failure, so a ClickHouse
  output isn't affected.

The consumer has two properties that constrain a background reader:

- `KafkaConsumer`'s read-order bookkeeping (`_next_seq`,
  `_oldest_outstanding`) lives in plain Python attributes, with no lock.
- The native consumer holds a mutex for each `POLL_STEP` (100 ms) of a poll.
  A commit from another thread waits for it.

## Goal

Done when:

- **Delivery failures are detected.** The native producer reports delivery
  per message. A message librdkafka gives up on fails its batch, which is
  rewound, not committed. This holds for the synchronous loop too, not only
  the pipelined one.
- **Read-ahead:** while the node processes batch N, the harness is already
  polling and parsing batch N+1. It is transparent to existing nodes: they
  gain it with no code change, and `done()` keeps its synchronous meaning for
  them. How many batches are read ahead is capped, so memory stays bounded.
- **Asynchronous finishing, in its own class.** Today's `Node` becomes
  `SyncNode`, unchanged in behaviour, and a sibling `PipelinedNode` is added.
  A node chooses one by the class it constructs.
  - `PipelinedNode.done()` sends the output and returns without waiting for
    delivery.
  - Work that must follow the commit is passed to `done()` as a callback, and
    runs on the loop thread once the batch is committed.
  - A configurable limit caps how many finished batches may be waiting for
    delivery. When the limit is reached, `done()` blocks, which is how a slow
    output slows the input down.
- **Commit order.** Batches are committed in read order. Each is committed
  only after its output, and every earlier batch's output, has been
  delivered. After a failed delivery, the node stops and nothing from the
  failed batch onward is committed.
- **Both nodes pipelined, and node-dedup still exact.** node-el and
  node-dedup move to `PipelinedNode`. node-dedup's cross-batch dedup stays exact: a key sent in
  batch N is dropped from batch N+1 even while N's delivery is still pending.
- **Throughput.** With a Kafka input and output, node-el's throughput is
  measurably higher than 0.7.0's synchronous loop against the same local
  broker, and approaches the limit of whichever side is slower.
- **The perf report still makes sense when work overlaps.** Time the loop
  spent blocked, whether waiting for input or for in-flight capacity, is
  reported separately from work done in the background. "Starved" means the
  loop waited for input.
- **SIGTERM and SIGINT.** The node still stops after the batch in hand.
  Batches it finished are delivered and committed before the node exits.
  Read-ahead batches it never saw are left uncommitted, to be read again.
- **Release.** All of it ships as 0.8.0. `MIGRATION.md` tells node authors
  and `Producer` implementers what to change, starting with the
  `Node` → `SyncNode` rename.
- **Tests.** The in-memory doubles in `tkati_core.testing` can hold back,
  release out of order, or fail deliveries. The ordering, failure and stop
  guarantees are tested there, without a broker.

Non-goals:

- **Running node code concurrently.** The loop body still handles one batch
  at a time, on one thread. Pipelining overlaps the harness's I/O with that
  body, not two batches of node logic with each other.
- **Changing what `done()` means on `SyncNode`.** Code after
  `SyncNode.done()` keeps running after the commit, exactly as with today's
  `Node.done()`.
- **Multiple inputs or outputs, and exactly-once delivery.**
- **A node-facing API in Rust.** Node code stays Python. The parts that must
  run without the GIL go in the native extension, as the poll, parse and
  encode paths already do.
- **Background inserts for `ClickhouseProducer`.** It stays synchronous, so a
  ClickHouse output gains from read-ahead but not from overlapping delivery.
  See the open questions.

## Approach

The work splits into three pieces. Each is useful on its own, and each is a
prerequisite for the next.

### 1. Per-message delivery reports

The native producer tags each message with its batch when it enqueues it.
librdkafka's `BaseRecord` takes a delivery opaque. `StderrContext::delivery`
stops discarding reports: it counts delivered and failed messages per batch,
behind a mutex that the Python side reads. `enqueue` returns, or accepts, a
token for its batch. `Producer` gains a way to ask whether a token's messages
are all delivered, and whether any failed.

The synchronous `done()` uses this first. After `flush()` it checks the
batch's token, and raises if any message failed. The batch is then rewound
instead of committed. That closes the silent-failure gap before any
pipelining exists, and it gives the next two pieces their per-batch delivery
state.

`ClickhouseProducer` needs no token: its insert has either succeeded or
raised by the time `produce_arrow` returns.

### 2. Read-ahead

A reader thread owned by the harness calls the consumer's read method, the
one `consume_arrow()` or `consume_pylist()` chose, and puts each
`ConsumedBatch` on a bounded queue. The loop takes batches from the queue
instead of reading directly. The poll already runs natively without the GIL,
and most of pyarrow's JSON parsing does too, so the reader mostly overlaps
with the node's own Python code rather than competing with it.

This is invisible to node code. `done()` still flushes and commits before it
returns, so the code after it keeps its meaning. The commit names the batch's
own offsets, so it never covers a batch sitting in the queue.

Moving reads to a thread has consequences that the design has to handle:

- **Consumer calls from two threads.** The reader calls `read_*`; the loop
  calls `commit` and `rewind`. `KafkaConsumer`'s sequence bookkeeping needs a
  lock, or reads and commits need to be serialized through the harness.
- **Commit latency.** A commit waits up to one `POLL_STEP` for the native
  consumer's mutex while the reader is polling. That's acceptable at 100 ms,
  but it's worth measuring.
- **Signals.** Handlers run only on the main thread. The loop waits on the
  queue in short steps, so a signal is still handled promptly. A stop drains
  nothing from the queue: those batches were never handed out, so they aren't
  committed, and they are read again after a restart.
- **Rewind.** When a batch fails, `rewind` seeks back, which also
  invalidates every batch read after it, as 0.6.0 specifies. The harness
  clears the queue at the same moment. The reader must not hand out anything
  read before the seek.

### 3. Asynchronous finishing

Pipelining is a class, not a flag or a setting. The two modes differ in what
`done()` promises the code after it: with `SyncNode` the batch is committed
when `done()` returns; with `PipelinedNode` it isn't. Code written for one is
wrong under the other. node-dedup's mark-seen line after `done()`, for
example, would mark keys seen before delivery under `PipelinedNode`, and that
can lose events on a crash. So the mode belongs in the type:

- `SyncNode` and `PipelinedNode` are siblings on a private base that holds
  everything they share: consuming, the reader thread, stats, signals,
  lifecycle and `stop()`. Each class keeps only its own `done()` and commit
  bookkeeping.
- Neither subclasses the other. A function written as `run(node: SyncNode,
  ...)` rejects a `PipelinedNode` under `ty`, which a flag can't enforce.
- Switching a node is one visible diff: change the class, and move its tail
  work into callbacks.

`PipelinedNode.done()`:

1. sends the output;
2. records the batch as finished, with its delivery token and its callbacks;
3. returns, unless the number of finished-but-uncommitted batches has reached
   the in-flight limit. In that case it first waits for the oldest one to be
   committed.

Batch states become: *finished* (`done()` called) → *delivered* (all of its
messages acked) → *committed*.

The harness processes delivered batches on the loop thread, whenever the node
calls `done()` or asks for the next event:

- It commits delivered batches in read order, stopping at the first batch not
  yet delivered. Deliveries can finish out of order, but commits never do.
  This is the prefix tracker the 0.6.0 doc anticipated, and it matches the
  read-order check `KafkaConsumer.commit` already enforces.
- After each commit it runs that batch's `after_commit` callbacks, in order.
  Callbacks never run while the loop body is running, so node state needs no
  locking.
- If a batch's delivery failed, it rewinds the oldest uncommitted batch. That
  invalidates everything read after it, so the harness clears the read-ahead
  queue too. It then raises the failure out of the node's `for` loop, so the
  process exits just as it would for a failure inside the body.

**Callbacks.** `PipelinedNode.done()` takes `after_commit=`, and only that.
`SyncNode.done()` takes none: code after it already runs after the commit. Neither node
needs a callback that runs after delivery but before the commit, because
node-dedup already marks keys seen after the commit (the order the harness
review settled on). `on_delivered=` can be added when a node needs it.

**node-dedup under pipelining.** Its mark-seen and dropped-rows counter move
into `after_commit`. Until a batch is committed, the keys it sent are neither
in the store nor visible to the next batch's lookup. To keep cross-batch
dedup exact, dedup checks each new batch against the store plus an in-memory
set of keys that have been sent but not yet committed. The `after_commit`
callback moves a batch's keys from that set into the store. If the process
crashes, the set and the uncommitted offsets are lost together. The re-read
batches find their keys unseen, and send the rows again, so the worst case
is a duplicate, never a lost event.

node-el just switches to `PipelinedNode`. It has no work after `done()`.

**Stop.** On a stop, whether a signal, `stop()` or `stop_when_idle`, the
harness:

1. stops the reader and drops the queued batches;
2. waits for every finished batch to be delivered;
3. commits them in order and runs their callbacks;
4. closes everything.

A batch the node was still processing when the stop arrived must still be
finished with `done()`, as today.

### Stats

Pipelining means the phases are no longer serial, so shares of the interval
stop adding up the way they do today. The report separates two kinds of time:

- **Loop blocked**: waiting for the next batch from the reader, and waiting
  in `done()` for in-flight capacity. "Starved" becomes "the loop waited for
  input".
- **Background**: the reader's `consumer/*` phases, and delivery.

`LoopStats` is written from two threads. Each phase is only ever recorded by
one of them, so the unlocked single-key updates stay safe, but this needs
confirming, and documenting in `stats.py`.

### Tests

The in-memory doubles grow the controls the pipelined paths need:

- `MemoryProducer` holds deliveries until the test releases them, can release
  them out of order, and can fail one.
- `MemoryConsumer` can be read from the reader thread.

Harness tests cover:

- commits in read order under out-of-order acks;
- no commit past a failed delivery;
- `done()` blocking at the in-flight limit;
- read-ahead dropped on stop and on rewind;
- callbacks running on the loop thread, after the commit.

node-dedup gets a test that a key sent in batch N is dropped from batch N+1
while N is still undelivered.

## Decisions

The Approach left these open. These are the choices the Implementation Steps
build on, for review:

- **Limits.** Two settings, both counted in batches, since `batch_size`
  already bounds a batch's size. `read_ahead` (default 1; 0 turns the reader
  thread off and reads on the loop thread as today) and `max_in_flight`
  (default 4; used only by `PipelinedNode`). They live in a new `[pipeline]`
  section of `NodeSettings`. `read_ahead` applies to both classes, since it
  doesn't change what `done()` means.
- **Two classes, `SyncNode` and `PipelinedNode`.** They are siblings on a
  private `_NodeBase`, not a `pipelined=` flag (see **3. Asynchronous
  finishing**). `Node` is renamed to `SyncNode`, with no alias left behind:
  an alias would make the choice implicit again. `after_commit=` exists only
  on `PipelinedNode.done()`.
- **Release.** One release, 0.8.0, at the end of all phases. The rename and
  the `Producer` interface change are breaking, and `MIGRATION.md` gets a
  0.8.0 section for them.
- **Delivery token.** The harness tags each batch's output with a number
  derived from the batch's `seq` (`seq + 1`, since 0 means untracked). `Producer.produce_arrow` / `produce_pylist` gain `tag: int | None =
  None`, and `Producer` gains `wait_delivered(tag, timeout)`. It returns
  `True` once every message with that tag is acked, `False` on timeout, and
  raises `DeliveryError` if any failed. `ClickhouseProducer` returns `True`
  at once, because its inserts are synchronous. Untagged sends, such as the
  DLQ fallback's, are not tracked.
- **Rebalances.** Accepted as duplicates for now. The new owner re-reads what
  was in flight, and at-least-once holds. A revocation callback that drains
  and commits first is a later refinement, and only worth it if duplicates
  show up in practice.
- **Stats.** One `LoopStats`, with two new phases the loop records while it
  is blocked: `wait/input`, when the read-ahead queue is empty, and
  `wait/in-flight`, when `PipelinedNode.done()` is at the limit. The reader
  thread keeps recording `consumer/*`. Both new phases become required
  columns for the class that records them, so a node that passes its own
  `phases=` tuple has to add them (a `MIGRATION.md` item). Background and loop phases can overlap, so the
  percentages may add up to more than 100%. The README explains how to read
  them.
- **ClickHouse.** Stays synchronous.

## Implementation Steps

Phases 0 and A–C, each its own jj change, in this order, then one 0.8.0
release (Phase D). Each phase leaves the tree working and tested.
`MIGRATION.md` grows as the phases land, one item per breaking change, so the
release step only has to review it.

### Phase 0: rename `Node` to `SyncNode`, on a shared base

1. **`tkati_core/node.py`.**
   - Split `Node` into `_NodeBase` and `SyncNode(_NodeBase)`.
   - `_NodeBase` keeps everything but finishing: `__init__`,
     `from_settings`, `__enter__` / `__exit__`, `consume_arrow` /
     `consume_pylist` and the generator behind them, `_read`, `phase`,
     `stop`, `stats`, and the signal handlers.
   - `SyncNode` gets `done()`. `from_settings` returns `Self`, so it builds
     the right class.
   - The outstanding-batch check that `__exit__` rewinds with moves behind a
     `_oldest_uncommitted()` hook, which each class implements. For
     `SyncNode` it's the outstanding batch.
   - No `Node` alias.
2. **Exports.** `tkati_core/__init__.py` exports `SyncNode` instead of
   `Node`. `tkati_core.testing.memory_node` builds a `SyncNode`.
3. **Call sites.** Both nodes' `main.py` and tests, `test_node.py`, and the
   node READMEs use `SyncNode`.
4. **Docs.**
   - The tkati-core README's section on the harness becomes "`SyncNode`".
   - The harness doc gets a one-line note that `Node` is `SyncNode` from
     0.8.0.
   - Start `MIGRATION.md`'s 0.8.0 section with the rename:
     `from tkati_core import Node` → `SyncNode`, with the same API.

### Phase A: per-message delivery reports

1. **`packages/tkati-core/src/kafka.rs`**
   - `StderrContext` gets a `deliveries: Mutex<HashMap<u64, TagState>>` plus
     a `Condvar`. `TagState` holds `pending: usize` and `failed:
     Option<String>`. `DeliveryOpaque` becomes `usize`, and `0` means
     untracked.
   - `delivery()` decrements `pending` for its tag, records the first error,
     and notifies the condvar.
   - `KafkaProducer::enqueue(messages, tag)` adds `messages.len()` to the tag's
     `pending` before sending. It sends with
     `BaseRecord::with_opaque_to(&self.topic, tag)`, and on `QueueFull` backs
     the count off for any message that wasn't sent.
   - New `wait_delivered(tag, step) -> Result<Option<bool>, String>`. It waits
     up to `step` on the condvar and returns `Some(true)` when pending reaches
     0, removing the tag. It returns `Some(false)`, also removing the tag,
     when a message failed, and `None` on timeout.
   - Unit tests cover the bookkeeping (pending counts, first error kept, tag
     removed once settled) without a broker.
2. **`packages/tkati-core/src/lib.rs`**
   - `NativeProducer.enqueue(batch, tag=0)`.
   - `NativeProducer.wait_delivered(tag, timeout: float | None) -> bool`. It
     loops in `POLL_STEP` waits with the GIL released, calling
     `check_signals()` between them, and raises `DeliveryError`, a new Python
     exception class, with the recorded error.
   - Update `tkati_core/_native.pyi` to match.
3. **`packages/tkati-core/tkati_core/producer.py`**: `produce_arrow` /
   `produce_pylist` take `tag: int | None = None`. New abstract
   `wait_delivered(tag: int, timeout: float | None = None) -> bool`, whose
   docstring states the contract above. Export `DeliveryError` from
   `tkati_core`.
4. **`kafka/producer.py`**: pass `tag` (or 0) through to `enqueue`, and
   implement `wait_delivered` over the native one.
   **`clickhouse/producer.py`**: accept `tag`, and have `wait_delivered`
   return `True`.
5. **`tkati_core/node.py`**:
   - `SyncNode.done()` tags its output with `batch.seq + 1`.
   - After `flush()`, it calls `wait_delivered(tag, 0)`. A `DeliveryError`
     propagates like any other failure in `SyncNode.done()`, so the batch is rewound,
     not committed. This closes the silent-failure gap.
6. **`tkati_core/testing.py`**: `MemoryProducer` records tags, and
   `fail_delivery={tag: exc}` makes `wait_delivered` raise for that tag.
7. **Tests.**
   - `test_node.py`: a failed delivery in `done()` rewinds and raises.
   - `test_producer.py`: against Redpanda, produce with `message.timeout.ms`
     set very low to a topic that doesn't exist, with auto-creation off for
     that producer, and check that `wait_delivered` raises.
8. **Docs.**
   - The tkati-core README's producer section documents tags and
     `wait_delivered`.
   - The harness doc's Context line about unnoticed failures gets a note that
     this phase fixed it.
   - Changelogs.
   - `MIGRATION.md` items: `Producer` implementations accept `tag=` and
     implement `wait_delivered`; `DeliveryError` now fails a batch whose
     delivery failed, where before it passed unnoticed.

### Phase B: read-ahead

1. **Settings.** `tkati_core/settings.py` gets
   `PipelineSettings(read_ahead: int = 1, max_in_flight: int = 4)`, with
   validators (`read_ahead >= 0`, `max_in_flight >= 1`), and
   `NodeSettings.pipeline: PipelineSettings = PipelineSettings()`.
   `_NodeBase.__init__` takes `read_ahead=`, and `from_settings` passes it
   from `settings.pipeline`, for both classes.
2. **`kafka/consumer.py`**: a `threading.Lock` around the sequence
   bookkeeping in `_wrap`, `commit` and `rewind`, because the reader thread
   calls `read_*` while the loop commits.
3. **The reader in `tkati_core/node.py`**:
   - `_consume(read)` starts a daemon `threading.Thread` running
     `_reader(read)` when `read_ahead > 0`, the first time the node is
     consumed. It uses a `queue.Queue(maxsize=read_ahead)`.
   - `_reader` loops: it calls `read(...)` and puts the result on the queue.
     It uses `put` with a timeout, checking a `_reader_stop` event between
     tries, so it never blocks past a stop. It puts an exception from `read`
     on the queue, so the loop re-raises it. It exits when the stop event is
     set or the consumer reports it's closed.
   - `_read(read)`, with read-ahead on, becomes `_take()`. It calls
     `queue.get` with `timeout=POLL_STEP` in a loop, returns `None` as soon
     as a stop is requested, and times its waiting into `wait/input`. With
     read-ahead off, today's `_read` and `_Stop` path stay as they are.
   - Add `wait/input` to `DEFAULT_PHASES` and `SINK_PHASES`, and to
     node-dedup's `_PHASES`.
4. **Stop, failure and exit.** One helper, `_stop_reader()`: set the event,
   join the thread with a timeout of one `batch_timeout_sec`, and drop the
   queue.
   - Call it before any `rewind` in `__exit__`. A read still in progress
     could otherwise hand out messages from before the seek, under a
     committable `seq`.
   - Call it before closing the consumer.
5. **`tkati_core/testing.py`**: make `MemoryConsumer` safe to read from the
   reader thread (a lock around the iterator and the counters), and give it
   an optional `delay` per read.
6. **Tests.** In `test_node.py`, run the existing suite with `read_ahead=0`
   and with `read_ahead=1` (parametrize `memory_node`). Add:
   - the reader reading batch N+1 while the body still holds batch N;
   - a stop dropping queued batches, which are neither committed nor
     rewound;
   - a rewind stopping the reader before seeking;
   - an exception in `read` surfacing in the loop;
   - SIGTERM while the loop waits on an empty queue ending promptly.
7. **Docs.**
   - README: read-ahead, the `[pipeline]` section, and `wait/input`.
   - Changelogs.
   - `MIGRATION.md` item: a node that passes its own `phases=` tuple adds
     `wait/input`.

### Phase C: asynchronous finishing

1. **`PipelinedNode(_NodeBase)` in `tkati_core/node.py`.** Its
   `__init__` / `from_settings` also take `max_in_flight=`, with
   `from_settings` reading it from `settings.pipeline`. Its `done()` has the
   same signature as `SyncNode.done()`, plus
   `after_commit: Callable[[], object] | None = None`. Export it from
   `tkati_core`.
2. **Finished batches.**
   - A `_Finished` record holds the `ConsumedBatch`, its tag, the rows out,
     and the callbacks. It lives in `self._finished: collections.deque`.
   - `PipelinedNode.done()` sends with the tag, appends a `_Finished`, clears
     the outstanding batch, then calls `_settle(block=len(self._finished) >=
     max_in_flight)`.
   - `_settle(block)` commits from the head while
     `wait_delivered(head.tag, 0)` is true. Each commit is timed into
     `commit`, adds the rows to `rows_out`, runs the callbacks, and pops the
     head. When `block` is set, it waits on the head, timed into
     `wait/in-flight`, until the deque is under the limit.
   - `_next_event` calls a `_before_read()` hook before reading. It does
     nothing on `SyncNode`; `PipelinedNode` calls `_settle(block=False)`
     there.
3. **Stop and failure.**
   - On a stop (step 3 of the iteration, and the stop paths in
     `_next_event`), call `_settle` until the deque is empty, then end.
   - A `DeliveryError` from `_settle` propagates. `__exit__` then rewinds
     `_oldest_uncommitted()`, which on `PipelinedNode` is the deque's head
     if it has one, and otherwise the outstanding batch. It calls
     `_stop_reader()` first.
   - With `KeyboardInterrupt`, drain nothing.
4. **Stats.** `PipelinedNode`'s default and required phases add
   `wait/in-flight`, as a new `PIPELINED_PHASES` constant next to
   `DEFAULT_PHASES`. node-dedup's `_PHASES` adds it too. Add the
   "custom `phases=` tuples must include `wait/in-flight` on
   `PipelinedNode`" item to `MIGRATION.md`.
5. **`packages/tkati-node-dedup/src/tkati_node_dedup/store.py`**:
   `filter_duplicates(keys, pending: Collection[bytes] = ())` treats keys in
   `pending` as already seen.
6. **`tkati_node_dedup/main.py`**:
   - `run` keeps a `pending: set[bytes]` and passes it to `_dedupe_batch` /
     `filter_duplicates`.
   - After `done()`, it adds `new_keys` to `pending` right away.
   - `after_commit` runs a small `_mark_seen(store, pending, new_keys,
     dropped)`, which calls `store.add_many`, removes the keys from
     `pending`, and increments `_DROPPED_ROWS`.
   - `run` takes a `PipelinedNode`, and `main()` builds one with
     `PipelinedNode.from_settings`. Update the module docstring's crash
     analysis to cover the pending set.
7. **`tkati_node_el/main.py`**: `run(node: PipelinedNode)` and
   `PipelinedNode.from_settings`, with no other change.
8. **`tkati_core/testing.py`**:
   - `memory_node(..., node_cls=SyncNode)` also builds a `PipelinedNode`.
     The tests that apply to both classes are parametrized over it.
   - `MemoryProducer(deliver="immediate" | "manual")` with
     `release(tag)` / `fail(tag, exc)`.
   - An `on_wait(tag)` hook that the harness triggers when it blocks, so a
     single-threaded test can release or fail a delivery at exactly that
     point.
9. **Tests.**
   - `test_node.py`:
     - commits in read order when deliveries are released out of order;
     - no commit past a failed delivery, with the oldest uncommitted batch
       rewound;
     - `done()` blocking at `max_in_flight`, and unblocking once the head is
       released;
     - `after_commit` running after the commit, and never for an
       uncommitted batch;
     - `ty` rejecting a `PipelinedNode` passed where a `SyncNode` is
       expected. This is a type-check fixture, not a runtime test;
     - stop draining every finished batch.
   - `test_node_dedup.py`:
     - a key in batch N is dropped from batch N+1 while N is undelivered;
     - a crash with batches in flight re-sends them after the restart.
10. **Benchmark.** A script next to `benchmarks/bench_kafka_json.py` runs
    node-el Kafka to Kafka against local Redpanda for a fixed message count,
    three ways:
    - `SyncNode` with `read_ahead=0` (the 0.7.0 behaviour);
    - `SyncNode` with `read_ahead=1`;
    - `PipelinedNode`.

    It reports rows per second. The numbers go in this doc's Verification.
11. **Docs.**
    - README: `PipelinedNode`, when to choose it over `SyncNode`,
      `after_commit`, and how to read the perf line when phases overlap.
    - The harness doc's "Later" section says this is built.
    - Changelogs.

### Phase D: release 0.8.0

1. **Version.** Bump every version to 0.8.0 per AGENTS.md: the root and
   package `pyproject.toml`s, the `tkati-core==` pins, and
   `packages/tkati-core/Cargo.toml`. Run `uv sync --all-packages`, and check
   with the one-line `grep`.
2. **`MIGRATION.md`**: review the 0.8.0 section as a whole. It should cover:
   - `Node` → `SyncNode`, same API;
   - `PipelinedNode`: what changes when moving to it, and that code after
     `done()` must move into `after_commit=`;
   - `Producer` implementations: accept `tag=` on `produce_arrow` /
     `produce_pylist`, and implement `wait_delivered`;
   - custom `phases=` tuples: add `wait/input`, and `wait/in-flight` on
     `PipelinedNode`;
   - `DeliveryError`: failures that used to pass unnoticed now fail the
     batch;
   - the optional `[pipeline]` settings section.
3. **Changelogs**: the 0.8.0 root entry and each package's entry.
4. **This doc**: set `status` to `IMPLEMENTED`, and add "Implementation notes
   (as built)" and "Verification", including the benchmark numbers.

Prepare the local commit only; the tag is pushed by the maintainer.

Across all phases, run `uv run ruff check packages/`,
`uv run ty check packages/`, the cargo checks in `packages/tkati-core` and
`uv run pytest` for tkati-core and both nodes against local Redpanda and
ClickHouse. Also repeat the node-el SIGTERM check: the committed offset must
equal the output count after a stop, with batches in flight.

## Implementation notes (as built)

Built in five jj changes, as planned: the rename (Phase 0), delivery reports
(A), read-ahead (B), `PipelinedNode` (C) and the 0.8.0 release (D). Where the
code differs from the steps above, or goes beyond them:

- **`flush()` cost 100 ms per call (fixed in Phase A).** The Phase C benchmark
  showed `SyncNode` at about 9.4k rows/s whatever the read-ahead, which is one
  batch every ~105 ms. `KafkaProducer.flush()` called librdkafka's flush in
  `POLL_STEP` (100 ms) steps; with a `ThreadedProducer` the delivery reports
  are served by its own background thread, so the flush call found nothing to
  serve and returned only when its timeout ran out. Every flush since 0.5.0
  cost at least 100 ms. `Deliveries` now also counts every message in flight,
  tagged or not, and `flush()` waits on the same condvar until that count is
  zero: about 6 ms for a 1000-row batch against a local broker.
- **The native consumer's lock (Phase B).** Two changes the plan didn't
  foresee, both found by timing the read-ahead thread:
  - The consumer's `Mutex` became an `RwLock`. Polling, committing and
    seeking take the read side, which librdkafka allows from several threads
    at once. With the `Mutex`, a commit waited behind the reader's poll steps,
    for up to 3 s.
  - `close()` sets a `closing` flag before it takes the write lock, and every
    call checks the flag first. Without it, a reader re-taking the read lock
    every 100 ms kept `close()` waiting for its whole batch timeout (up to
    10 s).
  - `test_commit_and_close_do_not_wait_for_a_concurrent_poll` in
    `test_consumer.py` pins both behaviours.
- **End of in-memory input (Phase B).** With read-ahead, the test doubles'
  "stop when the batches run out" would drop batches already queued, as a
  real stop does. A private `_NodeBase._end_of_input()`, called from inside
  the read that found nothing left, makes the loop end in order after them.
  `memory_node` / `memory_pipelined_node` wire it to `on_exhausted`.
- **`LoopStats.record` takes the stats lock (Phase B)**, because the reader
  records phases too. Its get-then-set could otherwise straddle a `reset()`
  on the loop thread.
- **`SyncNode(...)` constructed directly defaults to `read_ahead=0`**, and
  only `from_settings` reads `[pipeline] read_ahead` (default 1). Tests and
  code that build nodes by hand keep 0.7.0's behaviour unless they ask.
- **Exit drains on any clean exit (Phase C)**, including a `break` after
  `done()`, not only on a stop. Draining happens inside `__exit__`'s
  `try`/`finally`, so a second Ctrl-C while draining still closes everything.
- **Stopping the reader before a rewind can wait up to one batch timeout.**
  The reader's poll can only be interrupted by closing the consumer, which a
  rewind can't do. This only affects the failure path, and if the reader
  doesn't stop in time the rewind is skipped: the batch is uncommitted either
  way, so it is read again after a restart.
- **Test doubles.** `memory_pipelined_node(...)` is a second factory, not a
  `node_cls=` argument to `memory_node`, so each one returns a precisely typed
  node. `MemoryProducer` logs `wait:<tag>` only for waits that may block.
- **Read-ahead tests.** Rather than running the whole harness suite at every
  `read_ahead`, the ordering, rewind and delivery-failure guarantees are
  parametrized over it. Tests that assert on the exact interleaving of reads
  and commits stay at `read_ahead=0`, because read-ahead changes that
  interleaving by design.
- **The type-check fixture** (`_type_check_fixture` in `test_node.py`) passes
  a `PipelinedNode` where a `SyncNode` is expected, under
  `# ty: ignore[invalid-argument-type]`. If the two classes ever become
  compatible, ty reports the ignore as unused.
- **Still untracked:** `ClickhouseProducer`'s DLQ fallback produces to the DLQ
  without a tag. A DLQ message that fails delivery is waited for by `flush()`,
  but not reported.

## Verification

- `uv run ruff check packages/` and `uv run ty check packages/` are clean.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and
  `cargo test` pass. That includes the new `Deliveries` tests: per-tag
  settling, the first failure winning, cancelled messages, a waiter woken by
  a report, and `wait_all`.
- `uv run pytest packages/tkati-core packages/tkati-node-el packages/tkati-node-dedup packages/tkati-dashboard`
  passes against local Redpanda and ClickHouse (250 tests). 70 of them are
  broker-free harness tests in `test_node.py`. The read-ahead tests ran 20
  times in a row without a failure.
- Broker tests:
  - A message larger than Redpanda accepts makes `wait_delivered` raise
    `DeliveryError` after `flush()`.
  - Read-ahead delivers and commits every batch in order.
  - `consume_pylist` skips a malformed message and commits past it.
  - Commit and close don't wait for a concurrent poll.
- `benchmarks/bench_node_pipeline.py`: node-el's loop, Kafka to Kafka, 200k
  rows in batches of 1000, best of 3, against local Redpanda:

  | Mode | rows/s |
  |---|---|
  | `SyncNode`, `read_ahead=0`, before the `flush()` fix (as in 0.7.0) | ~9,400 |
  | `SyncNode`, `read_ahead=0` | ~89,700 |
  | `SyncNode`, `read_ahead=1` | ~96,600 |
  | `PipelinedNode`, `read_ahead=1`, `max_in_flight=4` | ~128,000 |

- SIGTERM checks: node-el (`from_settings`) with input flowing, stopped
  mid-stream:
  - With read-ahead on `SyncNode`: exit code 0 within about 270 ms, and the
    committed offset equalled the output count (56,000).
  - As a `PipelinedNode`, run twice: exit code 0 within about 260 ms, with
    316,000 and 317,000 committed and the same counts in the output topic.
