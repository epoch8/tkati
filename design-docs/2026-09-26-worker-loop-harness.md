---
status: IMPLEMENTED
---

# Worker loop harness

## Context

Each node package writes its own main loop. As of 0.6.0, `tkati_node_el.main`
and `tkati_node_dedup.main` do the same things around their node-specific
logic:

- `main()` loads `AppSettings`, then calls `build_consumer(settings.input)`,
  `build_producer(settings.dlq)` and `build_producer(settings.output, dlq_producer=...)`.
- It builds a `LoopStats` from a `_PHASES` tuple and calls
  `start_metrics_server`.
- It runs `while True: run_one_iteration(...); stats.report_if_due()`.
- A `finally` closes the consumer, the output producer and the DLQ producer,
  in that order.
- `run_one_iteration` calls `consumer.read_arrow(..., stats=stats)`. It counts
  `iterations`, `starved_iterations` (no batch, or a short one) and `rows_in`.
  It calls `produce_arrow` and `flush` inside a `try` that calls
  `consumer.rewind(batch)` and re-raises on failure. Then it counts `rows_out`
  and calls `consumer.commit(batch)` inside a `commit` phase.

The node-specific part is small. In `tkati-node-el` it is nothing: the batch
goes straight from input to output. In `tkati-node-dedup` it is
`_dedupe_batch` plus `store.add_many` and `store.cleanup_expired`.

The two loops weren't always the same.
`design-docs/2026-09-26-node-el-loop-stats.md` describes how the 0.5.2 release
went into getting them there:

- node-el was still the 0.3.0 loop. It had no stats and no metrics, and it
  committed right after `produce_arrow` without a `flush`. With a Kafka output
  it could lose rows whose offsets were already committed, which broke its
  at-least-once guarantee.
- Neither node closed its output producer on shutdown.

That release fixed both problems by copying node-dedup's loop into node-el,
comments included, "so the two files read as siblings". Nothing keeps them
that way. The next change to either loop has to be made in both places, and a
third node would start from a copy of one of them.

That doc ruled out a shared runner, because "their loops differ in the middle
(lookup/write, store cleanup)". That is true, but the parts that differ are
all node logic. Everything around them is the same in both nodes, and the
surrounding code is where the bugs above were.

Two problems remain in both loops today:

- **Shutdown on SIGTERM.** Only Ctrl-C reaches the `finally`: the native
  `poll_batch`/`flush` call `py.check_signals()`, and a `KeyboardInterrupt`
  unwinds the stack. SIGTERM, which is what a pod gets on termination, still
  has Python's default handler, so the process exits without running the
  `finally`. For dedup this means `BucketedDedupStore.close()` does not run.
  The store runs with the WAL off, so keys added since the last memtable flush
  are lost (`test_hard_kill_loses_unflushed_keys_with_the_wal_off`), and
  duplicates of them get through after the restart.
- **Work on idle iterations.** `store.cleanup_expired()` has to run even when
  a poll returns nothing, or an idle node never expires buckets. It ends up at
  the top of `run_one_iteration`, before the read, and its time is booked
  under the `commit` phase. The loop has no concept of "a timer fired" or "the
  input was idle", so the work is fitted around the read call.

The tests are duplicated in the same way. Each node's tests call its own
`run_one_iteration` with `MagicMock` consumers and producers, and set
`settings.input.consumer.batch_size` by hand. Most of what they check is loop
behaviour, not node logic. Both packages now contain
`test_iteration_hands_its_stats_to_the_consumer_and_producer` and a "failed
flush doesn't commit" test (`test_failed_flush_rewinds_instead_of_committing`
and `test_crash_before_flush_does_not_mark_seen_or_commit`). With 0.6.0 each
of them also checks that `commit` and `rewind` get the batch as read, not the
node's filtered table.

[dora-rs](https://dora-rs.ai/) is a useful reference for the shape we want. A
dora node is an ordinary program that iterates over its events
(`for event in node:` in Python, `while let Some(event) = events.recv()` in
Rust). Each event is an input carrying an Arrow array, a timer tick or a stop
request, and the node calls `send_output` for what it emits. The runtime takes
care of transport, lifecycle, timers and shutdown, so the node code contains
only its own control flow. The main difference from tkati is that dora has no
durable offsets: its inputs are not acknowledged. tkati's at-least-once
guarantee depends on committing an input offset only after the output
produced from it is durable. So a harness for tkati also has to find out from
the node when it has finished with a batch, which dora never needs to know.

## Goal

Done when:

- `tkati-core` provides a harness that owns a node's input, output, DLQ,
  stats, metrics server and lifecycle, and gives node code a stream of batches
  to iterate over.
- `tkati-node-el` and `tkati-node-dedup` are rewritten on top of it. Their
  `main.py` contains settings, the node's own resources (the dedup store) and
  a loop over the stream. There is no `while True`, and node code no longer
  calls `build_consumer`, `build_producer`, `close`, `commit`, `rewind`,
  `start_metrics_server` or `report_if_due`.
- An input offset is committed only after every row sent from that batch has
  been delivered. The harness enforces this, so no node has to write
  flush-before-commit itself, and a new node can't leave it out the way
  node-el's 0.3.0 loop did.
- A node can require delivery before one of its own side effects: dedup
  marks keys seen only after the rows are delivered.
- Every node gets the perf log line and `/metrics` (phases, rows in/out,
  iterations, starved iterations) without writing any code for it. A node
  declares and times only its own phases (`lookup`, `write`).
- A node doesn't have to have an output producer. A node that reads Kafka and
  writes to a cloud API through its own client still gets the harness's
  read, commit, rewind, stats and lifecycle. It is then responsible for
  making its own writes durable before it marks the batch done.
- A node can do periodic or idle work, such as `cleanup_expired`, as an
  ordinary part of its loop, whether or not a batch arrived.
- SIGTERM and SIGINT both stop the node at a batch boundary. Finished work is
  flushed and committed, and every resource is closed: the harness closes its
  own, and the node's own get closed as well.
- A node's loop can be tested by giving the harness a list of in-memory batches
  and checking what was sent and what was committed, with no Kafka and no
  `MagicMock`.
- Tests of loop behaviour (no commit when flush fails, stats passed down, rows
  and starved iterations counted) exist once, in `tkati-core`. The node
  packages keep only tests of their own logic, such as no mark-seen when flush
  fails and dropped rows counted only after commit, written against the
  in-memory harness.

Non-goals:

- Multiple named inputs or outputs, or several nodes running in one process
  (dora's dataflow graph). A node keeps one input, one output and an optional
  DLQ, as it has today. The design shouldn't make named inputs impossible
  later.
- Pipelining, meaning polling batch N+1 while batch N is being processed, or
  committing asynchronously from delivery callbacks (the throughput fix that
  `design-docs/2026-09-26-node-el-loop-stats.md` mentions). The loop still
  handles one batch at a time and waits for acks every iteration. The design
  shouldn't rule either of these out. 0.6.0's per-batch `commit`/`rewind`
  (`design-docs/2026-09-26-explicit-batch-commit.md`) already provides the
  consumer side, and once the harness owns commit, the rest becomes a change
  in one place.
- Exactly-once delivery or Kafka transactions.
- Moving the loop into Rust. The hot paths (poll, parse, encode) are already
  native, and node logic runs in Python under the GIL either way.
- Changing the `Consumer`/`Producer` interfaces or the settings file layout
  beyond what the harness needs.
- A `Producer` implementation for HTTP or cloud APIs. A node without an
  output producer writes through its own client, and turning such clients
  into producers is left for when a second node needs the same one.
- Detecting per-message Kafka delivery failures. `StderrContext::delivery` in
  `packages/tkati-core/src/kafka.rs` discards delivery reports, so a message
  librdkafka gives up on still lets `flush()` return and its batch get
  committed. That gap is independent of who owns the loop and gets its own
  change.

## Approach

The harness takes over the loop, and the node keeps control of what it does
with each batch. As in dora, the boundary between them is Python's iteration
protocol. The node writes `for event in node:`. Each step of that iteration is
the harness polling the input, doing its bookkeeping and returning the next
event. The body of the loop is the node's logic, in the order the node wants
it.

We use an iterator rather than a callback API (`harness.run(process_batch)`).
The ordering that matters most in these nodes is send, then delivery and
commit, then the node's side effects. In a loop that order is visible from top
to bottom in the node's own code. State the node keeps between batches is just
local variables. Idle work is an `if`/`match` branch instead of a second
callback. The harness still owns everything that surrounds that ordering.

**Events.** The stream yields a small set of event types, which a node can
handle with `match`:

- A batch event: an Arrow table plus what the harness knows about it, such as
  whether the batch was short, meaning the poll drained the input and waited
  out the timeout.
- An idle event, for polls that return nothing, and possibly for timer ticks.

Stopping is not an event. When the harness is asked to stop, the iterator ends
and the `for` loop exits. dora needs a STOP event because its nodes have no
other place to clean up. In Python, `with` blocks around the loop handle that.

**Commit.** The node says explicitly when a batch is finished, and what it
produced, in one call: `node.done(event, output=table)`. `done()` sends the
output, flushes the producer, and then calls `consumer.commit(batch)` with
the `ConsumedBatch` it handed out. It returns once the commit is made. There
is no separate send, so every output is tied to the input batch it came from.
That tie is what a pipelined harness needs, to know which delivery lets which
batch commit. The harness keeps that object, and
the node only sees its data, so what gets committed is always the batch as
read, whatever the node filtered out of it.

For now `done()` is synchronous, so work that must follow delivery, or must
not repeat when a batch is re-read, simply goes on the lines after it. Dedup
marking keys seen and counting dropped rows are both examples. A node
without a producer writes to its destination itself, and calls
`done(event, rows_out=n)` once its own writes have finished.

Asking for the next event while a batch isn't done is a bug in the node, and
raises. If the body raises before `done()`, the harness calls
`consumer.rewind(batch)` and lets the exception out of the `for` loop. That
is what both nodes' `try`/`except` blocks do today, so the node code no longer
needs them. A `break` before `done()` leaves the batch uncommitted.

The first draft of this design committed implicitly, whenever the node asked
for the next event. That kept node code shorter, but it hid the most
important step of each iteration, and it had no point after the commit for
code that must run only then. An explicit `done()` states that step where it
happens. It is also the shape that grows into pipelining: `done()` will
return before delivery and take callbacks for the work after it (see
**Later: asynchronous `done()`** below).

**Lifecycle.** The harness is a context manager built from settings. It builds
the consumer, and also the output producer and the DLQ producer when the
settings configure an output. On exit it flushes and
closes all of them, in the order today's `finally` blocks use: consumer,
output, then DLQ, because the output can still route rows to the DLQ. It
installs a SIGTERM handler that works like SIGINT: a stop requested
while the node is processing takes effect at the next request for an event,
after the node has finished that batch with `done()`. A stop requested while the harness is
blocked in `poll_batch` or `flush` interrupts the wait through the
`check_signals` calls those functions already make. The node's own resources
go in the same `with` statement as the harness, so they are closed on every
exit path. There is no separate lifecycle-hook API.

**Stats and metrics.** The harness owns the `LoopStats`. It passes it to the
consumer and producer calls it makes, and it counts iterations, starved
iterations, rows in (per batch) and rows out (per `done()`) from the data it
handles. It also calls `report_if_due` and starts the metrics server from a
`metrics` settings section. A node declares its extra phases when it builds
the harness and times them through the harness. Node-specific counters, such
as dedup's `_DROPPED_ROWS`, stay in the node.

**Settings.** Both `AppSettings` classes already contain the same `input`,
`output`, `dlq` and `metrics` fields. The harness reads those fields from
a base settings model in `tkati-core`. Each node subclasses that model and adds
its own sections.

**Testing.** The harness can be built with an in-memory source and sink in
place of Kafka and ClickHouse. A node is structured as a function that takes
the harness plus its own resources and runs the loop, and `main()` calls that
function. Tests call the same function with an in-memory harness, give it
batches and check the sent rows and the committed positions. The harness's
own guarantees (commit after flush, rewind on exception, stop at a
boundary, stats passed down) are tested once, in `tkati-core`.

## Design

This section settles the questions the Approach left open. Names are final
unless review says otherwise.

### API

A new module, `tkati_core/node.py`:

```python
DEFAULT_PHASES = (*CONSUMER_PHASES, *PRODUCER_PHASES, "commit")
# The default for a node without an output producer.
SINK_PHASES = (*CONSUMER_PHASES, "commit")


@dataclass(frozen=True, slots=True, eq=False)
class Batch:
    """A batch read from the input. The harness keeps the ConsumedBatch; the
    node only sees what it needs."""

    data: pa.Table
    short: bool  # fewer rows than batch_size: the poll drained the input


class Idle:
    """A poll returned nothing."""


type Event = Batch | Idle


class Node:
    def __init__(
        self,
        consumer: Consumer,
        producer: Producer | None,
        *,
        batch_size: int,
        batch_timeout_sec: int,
        phases: tuple[str, ...] | None = None,
        dlq: Producer | None = None,
        metrics: MetricsSettings | None = None,
        handle_signals: bool = False,
        stop_when_idle: bool = False,
    ) -> None: ...

    @classmethod
    def from_settings(
        cls, settings: NodeSettings, *, phases: tuple[str, ...] | None = None
    ) -> "Node": ...

    def __enter__(self) -> "Node": ...
    def __exit__(self, *exc_info) -> None: ...
    def __iter__(self) -> Iterator[Event]: ...
    def __next__(self) -> Event: ...

    def done(
        self,
        event: Batch,
        *,
        output: pa.Table | None = None,
        rows_out: int | None = None,
    ) -> None: ...
    def phase(self, name: str) -> AbstractContextManager[None]: ...
    def stop(self) -> None: ...

    @property
    def stats(self) -> LoopStats: ...
```

The open questions from the Approach, settled:

- **Idle work is driven by empty polls only.** There are no timer events.
  Dedup runs `cleanup_expired` on every event, whether a batch or `Idle`. A
  busy node therefore cleans up on every batch, and an idle node on every
  empty poll, so neither needs a timer. Timer events can be added later as a
  third event type without breaking existing loops.
- **A batch event carries only `data` and `short`.** Offsets stay inside the
  harness. Neither node needs them, and exposing them would invite nodes to
  commit on their own.
- **`output` is Arrow only.** Both nodes use `read_arrow`/`produce_arrow`.
  The harness reads with `read_arrow`, so there is no `list[dict]` path to
  mirror.
- **Nodes time their phases through `node.phase(name)`.** `phases` is the
  full report order, as `_PHASES` is in each node today. It defaults to
  `DEFAULT_PHASES`, or to `SINK_PHASES` when `producer` is `None`. The
  harness requires it to contain every name the harness itself fills in: the
  consumer's phases, `commit`, and the producer's phases if there is a
  producer. It raises `ValueError` otherwise, so a node can't drop one of
  those columns. Because the
  node supplies the whole tuple, dedup's perf line keeps its current column
  order. `node.stats` exists for tests and for nodes that need a counter
  `LoopStats` already has.
- **The output producer is optional.** `producer=None` is for nodes that
  deliver their output themselves, for example through a cloud API client.
  Such a node owns delivery: everything it wrote for a batch must be durable
  before it calls `done()`, which commits the batch. See **Nodes without an
  output producer**.
- **A batch is finished explicitly, with a synchronous
  `node.done(event, output=table)`.** It sends the output, flushes, commits,
  adds the batch's rows to `rows_out`, and returns. Code after it runs after
  the commit, so there are no hooks. There is no `send` and no public
  `flush`, which keeps the interface minimal: every current node sends one
  table per batch, and none needs to wait for delivery without also
  committing. A node that needs multi-part output would bring back a
  lower-level send.
- **A node without a producer reports its rows with
  `done(event, rows_out=n)`.** `output` and `rows_out` are mutually
  exclusive: sent rows are already counted. `output` without a producer
  raises.
  - Asking for the next event while a batch isn't done raises
    `RuntimeError`. It is raised out of the `for` loop, so `__exit__`
    rewinds the batch.
  - `done()` twice raises too, and so does `done()` with an event that isn't
    the current batch.
  - An exception after `done()` propagates with nothing to rewind.

### Later: asynchronous `done()`

A later change will pipeline the loop. `done()` will then only mark the batch
finished and return, and delivery and commit will happen in the background.
The work that today sits on the lines after `done()` moves into callbacks on
it: `on_delivered` for what must follow delivery, and `after_commit` for what
must follow the commit. Until then, "the commit has been made when `done()`
returns" holds. Nodes written against it (dedup's mark-seen and counter) will
move their tail lines into those callbacks as part of that change.

### One step of the iteration

`Node.__next__` does the following, in order:

1. If a batch is outstanding, meaning the node didn't call `done()`, raises
   `RuntimeError`.
2. `stats.report_if_due()`.
3. If a stop was requested, ends the iteration (`StopIteration`, which it
   raises again on every later call).
4. Reads with `consumer.read_arrow(timeout=batch_timeout_sec,
   num_messages=batch_size, stats=stats)`. A stop signal that arrives during
   the read interrupts it (see **Signals**). A stop requested during the
   read, whether by a signal or by `stop()`, ends the iteration without
   handing anything out. A batch that was read is left uncommitted and is
   re-read on restart.
5. `stats.iterations += 1`. If the read returned `None`: `starved_iterations
   += 1`, and the harness either ends the iteration if `stop_when_idle` is
   set, or returns `Idle()`.
6. Otherwise:
   - `rows_in += len(data)`;
   - if the batch is short, `starved_iterations += 1`;
   - the batch becomes the outstanding one, and the harness returns
     `Batch(data, short)`.

`Node.done(event, output=, rows_out=)` does the finishing:
- checks that `event` is the outstanding batch;
- if `output` is a non-empty table, calls
  `producer.produce_arrow(output, stats=stats)`. An empty table or `None`
  sends nothing, which covers dedup's "everything was a duplicate" case;
- calls `producer.flush(stats=stats)` if there is a producer;
- calls `consumer.commit(batch)` inside the `commit` phase;
- adds `len(output)`, or `rows_out`, to `stats.rows_out`. This happens only
  at commit, which matches what both nodes do today;
- clears the outstanding batch.

If the send, flush or commit raises, the batch stays outstanding, and
`__exit__` rewinds it.

### Exit

`Node.__exit__` does the following:

- **Exception with a batch outstanding.** If the loop exits with an
  `Exception` (not a bare `BaseException`) while a batch is outstanding, it
  calls `consumer.rewind(batch)`. If the rewind itself fails, it logs the
  error and carries on, so the original exception isn't masked. A
  `KeyboardInterrupt` or `break` leaves the batch uncommitted without
  rewinding it. 0.6.0 chose the same behaviour so that a forced stop never
  waits on a seek.
- **Signal handlers.** It restores the handlers it replaced.
- **Closing.** It closes the consumer, the output producer and the DLQ, in
  that order, skipping whichever are absent, through a `contextlib.ExitStack`, so a failing close doesn't
  skip the ones after it.
- **Exceptions.** It never suppresses them.

The metrics server, when `metrics` is given, starts in `__enter__`, not in
`__init__`, so constructing a `Node` in a test starts nothing. As today, it
runs on a daemon thread and isn't shut down on exit.

### Signals

With `handle_signals=True`, which `from_settings` sets, `__enter__` installs
one handler for both SIGTERM and SIGINT. It does this only on the main thread,
and logs a warning and skips it anywhere else.

- **First signal.** The handler calls `stop()` and logs that the node will
  stop after the current batch. If the harness is inside step 4's read, the
  handler also raises a private `_Stop(BaseException)`. The native
  `poll_batch` calls `check_signals()` every 100 ms, which runs the handler,
  so the poll is cut short instead of running out the batch timeout.
  `_Stop` subclasses `BaseException` so that the consumer's own
  `except Exception` blocks let it through, and `__next__` catches it around
  the read.
- **Second signal.** It raises `KeyboardInterrupt`, as a forced stop.

A signal that arrives while the node's loop body is running only sets the
flag. The body finishes, the next `__next__` commits the batch in step 1, and
step 3 ends the loop.

### Settings

`tkati_core/settings.py` gains:

```python
class NodeSettings(TomlBaseSettings):
    input: InputSettings
    output: OutputSettings | None = None
    dlq: OutputSettings | None = None
    metrics: MetricsSettings = MetricsSettings()
```

`output` is optional in the base class. tkati-node-el and tkati-node-dedup
redeclare it as `output: OutputSettings` in their `AppSettings`, so their
config still fails validation without it. A model validator rejects `dlq`
without `output`: today the DLQ only receives rows the output producer
rejects, so a DLQ with no output would be accepted and then never used.

When `settings.output` is set, `Node.from_settings` builds the DLQ producer
first, then the output producer with `dlq_producer=` set to it, and then the
consumer. When `settings.output` is `None`, it builds only the consumer and
passes `producer=None`. If one of these builds
fails, it closes what it already built. It takes `batch_size` and
`batch_timeout_sec` from `settings.input.consumer`, and sets
`metrics=settings.metrics` and `handle_signals=True`.

### Test doubles

A new module, `tkati_core/testing.py`, which ships in the package so that node
packages can use it:

- **`MemoryConsumer(batches, *, on_exhausted=None, log=None)`**
  - `read_arrow` returns the tables in `batches` one by one, each wrapped in
    a `ConsumedBatch` with `BatchOffsets()` and an increasing `seq`. A
    `None` entry reads as an empty poll.
  - Once `batches` runs out, every read calls `on_exhausted` (if given) and
    returns `None`.
  - `commit`/`rewind` record the batch's `seq` in `commits`/`rewinds`. It
    also records the `stats` it was given and whether it was closed.
    `read_pylist` raises `NotImplementedError`.
- **`MemoryProducer(*, fail_flush=None, log=None)`**
  - Records the tables it's sent, how many times it was flushed, the `stats`
    it was given, and whether it was closed.
  - `flush` raises `fail_flush` when set.
- **`log`** is a list that both doubles append to (`"produce"`, `"flush"`,
  `"commit:0"`, `"rewind:0"`, `"close:consumer"`, …), so tests can check the
  order of operations across the two objects.
- **`memory_node(batches, *, batch_size=100, phases=None, output=True,
  fail_flush=None)`** returns `(node, consumer, producer)` with a shared log.
  With `output=False`, `producer` is `None` and the node has no producer.
  `on_exhausted` is wired to `node.stop()`, so a test's loop ends once the
  list is used up.

Tests against a real broker use `stop_when_idle=True` instead. The node then
processes everything that is already in the topic and stops at the first
empty poll, which is how the one-shot `run_one_iteration` tests behave today.

### Nodes without an output producer

A node that writes to a cloud API looks like this:

```python
def run(node: Node, client: ApiClient) -> None:
    for event in node:
        if isinstance(event, Batch):
            with node.phase("upload"):
                client.upload(event.data)  # returns once the API accepted it
            node.done(event, rows_out=len(event.data))


def main() -> None:
    settings = AppSettings()  # input, metrics and the API's own section
    with (
        closing(ApiClient(settings.api)) as client,
        Node.from_settings(settings, phases=(*CONSUMER_PHASES, "upload", "commit")) as node,
    ):
        run(node, client)
```

A client that buffers or sends in the background must wait for its writes
before `done()`, by calling something like `client.flush()` just before it. The harness can't flush a client it doesn't know
about. An API error raised in the body rewinds the batch like any other
exception.

### Nodes

tkati-node-el:

```python
def run(node: Node) -> None:
    for event in node:
        if isinstance(event, Batch):
            node.done(event, output=event.data)
            logger.debug(f"Produced {len(event.data)} rows")


def main() -> None:
    settings = AppSettings()
    with Node.from_settings(settings) as node:
        run(node)
```

tkati-node-dedup:

```python
def run(node: Node, store: BucketedDedupStore, field: str) -> None:
    for event in node:
        with node.phase("commit"):  # booked as before; see the comment there
            try:
                store.cleanup_expired()
            except Exception:
                logger.exception("dedup store cleanup failed; will retry next iteration")
        if not isinstance(event, Batch):
            continue
        table = event.data
        ...  # missing-field check and _dedupe_batch under node.phase("lookup")
        node.done(event, output=filtered)  # sent, delivered, then committed
        with node.phase("write"):
            store.add_many(new_keys)  # mark seen: after delivery
        _DROPPED_ROWS.inc(len(table) - len(filtered))  # after commit


def main() -> None:
    settings = AppSettings()
    store = BucketedDedupStore(...)
    with closing(store), Node.from_settings(settings, phases=_PHASES) as node:
        run(node, store, settings.dedup.field)
```

Dedup's order changes from deliver → mark seen → commit to deliver → commit →
mark seen. Both are safe: a key is never marked seen before its row is
delivered, which is the only order that could lose an event. They differ
only in which crash window lets a duplicate through:

- A crash between delivery and the next step re-reads and re-sends the
  batch, under both orders.
- A crash between the second and third steps:
  - old order: re-reads the batch and drops it, since its keys are already
    seen, so there's no duplicate;
  - new order: doesn't re-read the batch, and never marks its keys, so a
    later duplicate of one of them is forwarded.

That window is one memtable write, much narrower than the loss the WAL-off
store already accepts.

`closing(store)` is entered first, so the node exits and closes its clients
before the store closes. `cleanup_expired` now runs after the read rather than
before it. It still runs before the lookup, which is the ordering its comment
requires.

## Implementation Steps

All code goes in one new change on top of this doc's change. It is released
as 0.7.0: a new `tkati-core` API, plus a behaviour change on SIGTERM in both
nodes.

1. **`NodeSettings`**: in `packages/tkati-core/tkati_core/settings.py`,
   import `MetricsSettings` from `tkati_core.metrics` and add `NodeSettings`
   as in **Settings**, with `output` optional and the validator that rejects
   `dlq` without `output`. `metrics.py` doesn't import `settings.py`, so this
   creates no cycle.
2. **`tkati_core/node.py`**: `DEFAULT_PHASES`, `SINK_PHASES`, `Batch`,
   `Idle`, `Event`, `_Stop` and `Node`, with `producer` optional throughout, as in **API**, **One step of the iteration**, **Exit**
   and **Signals**. `Node.__next__` raises `RuntimeError` if it's called
   outside `with`. The module docstring explains the `done()` rule and
   `break`/exception behaviour.
3. **Exports**: in `tkati_core/__init__.py`, export `Batch`, `Idle`, `Node`,
   `NodeSettings`, `DEFAULT_PHASES` and `SINK_PHASES`.
4. **`tkati_core/testing.py`**: `MemoryConsumer`, `MemoryProducer` and
   `memory_node`, as in **Test doubles**.
5. **`packages/tkati-core/tests/test_node.py`**: broker-free tests of the
   harness, against the doubles:
   - `done()` flushes then commits, in that order in the log. Code after it
     runs after the commit. `rows_out` is counted at `done()`.
   - The next event without `done()` raises and rewinds. `done()` twice
     raises. `output` together with `rows_out` raises and rewinds.
   - An exception in the body rewinds the outstanding batch, doesn't commit
     it, propagates, and still closes everything. A failing rewind doesn't
     mask it. An exception after `done()` doesn't rewind.
   - A failing send or flush in `done()` rewinds and raises.
   - `break` before `done()` neither commits nor rewinds, but closes. `break`
     after `done()` keeps the commit.
   - `KeyboardInterrupt` in the body doesn't rewind.
   - An empty poll yields `Idle`. `stop_when_idle` ends the loop at the
     first empty poll instead.
   - An empty `output` doesn't reach the producer.
   - `stop()` in the body ends the loop after the current batch, without
     reading again.
   - The consumer and producer receive `node.stats`. `iterations`,
     `starved_iterations` (empty poll and short batch) and `rows_in`/`rows_out`
     are counted. `phases` missing a `DEFAULT_PHASES` name raises.
   - Close order is consumer, producer, DLQ, and a failing close doesn't skip
     the rest.
   - Without a producer (`memory_node(..., output=False)`):
     - `done()` commits the batch with no flush;
     - `output=` raises and rewinds;
     - `rows_out=` counts toward `rows_out` at commit;
     - the default phases are `SINK_PHASES`, and `phases` without the producer
       phases is accepted;
     - an exception in the body still rewinds.
   - `NodeSettings`:
     - without `output`, `from_settings` builds no producer;
     - `dlq` without `output` fails validation;
     - tkati-node-el's `AppSettings` still requires `output`.
   - With `handle_signals=True`:
     - `os.kill(os.getpid(), SIGTERM)` in the body behaves like `stop()`.
     - A SIGTERM during a read that sleeps interrupts it, and the loop ends
       well before the sleep would have.
     - A second signal raises `KeyboardInterrupt`.
     - The previous handlers are restored on exit.
6. **tkati-node-el**:
   - `settings.py`: `class AppSettings(NodeSettings)` redeclares
     `output: OutputSettings` as required.
   - `main.py`: replace `run_one_iteration`, `_PHASES` and `_new_stats` with
     `run(node)` and the new `main()`, as in **Nodes**.
7. **tkati-node-el tests**:
   - In `test_node_el.py` and `test_node_el_kafka_roundtrip.py`, the
     integration tests build
     `Node(consumer, producer, batch_size=..., batch_timeout_sec=..., stop_when_idle=True)`
     and call `run(node)` inside `with node:`.
   - The ClickHouse tests create their producer with
     `ClickhouseProducer.from_output_settings(...)`, not from the `ch_client`
     fixture. The node now closes its producer, and the fixture's client is
     still needed for the checks afterwards.
   - Delete the mock tests that moved to `tkati-core`:
     `test_iteration_hands_its_stats_to_the_consumer_and_producer`,
     `test_delivered_batch_is_committed`,
     `test_failed_flush_rewinds_instead_of_committing` and
     `test_iteration_counts_rows_and_starved_iterations`.
   - Add a `memory_node` test that every batch is sent unchanged.
8. **tkati-node-dedup**:
   - `settings.py`: `AppSettings(NodeSettings)` redeclares
     `output: OutputSettings` as required and keeps `dedup`.
   - `main.py`: `run(node, store, field)` and the new `main()`, as in
     **Nodes**, in the order deliver → commit → mark seen. `_PHASES`,
     `_dedupe_batch` and `_DROPPED_ROWS` stay. The comment on booking cleanup
     under `commit` moves with the code. The module docstring explains the
     new order and why it's safe.
9. **tkati-node-dedup tests**:
   - The Kafka tests call a `_run(test_settings, store)` helper. It builds a
     `Node` over a fresh consumer and producer with `stop_when_idle=True` and
     calls `run`. Tests that iterated twice call it twice, and the second
     consumer resumes from the first one's commit. The store stays open
     across both calls and is closed by the test.
   - The mock tests move to `memory_node`:
     - `test_crash_before_flush_does_not_mark_seen_or_commit` uses
       `fail_flush`, then runs a second node over the same table.
     - `test_dropped_rows_are_counted` and
       `test_failed_iteration_does_not_count_dropped_rows` stay.
     - `test_iteration_hands_its_stats_to_the_consumer_and_producer` is
       deleted, because `tkati-core` covers it.
   - Add a test that `cleanup_expired` runs on an `Idle` event: a
     `memory_node([None, table])` with a counting wrapper around the store
     method.
10. **Docs**:
    - `packages/tkati-core/README.md`: a `Node` section before `LoopStats`,
      covering the loop, the ack rule, signals, nodes without a producer (the
      cloud-API example from **Nodes without an output producer**) and
      `tkati_core.testing`. The
      `LoopStats` example becomes "what `Node` does for you, for a custom
      loop".
    - Node READMEs: SIGTERM/SIGINT stop after the current batch, and a
      second signal forces an exit. In tkati-node-dedup's README, the
      "graceful shutdown" paragraph now applies to pod termination too.
    - No `MIGRATION.md` entry, because nothing public is removed.
11. **Release 0.7.0**:
    - Bump per AGENTS.md: the root and package `pyproject.toml`s, the
      `tkati-core==` pins, and `packages/tkati-core/Cargo.toml`. Then run
      `uv sync --all-packages`, and check the one-line `grep` from
      AGENTS.md.
    - Add CHANGELOG entries for the new change: the root entry with its
      change id and `[tkati-core, tkati-node-el, tkati-node-dedup]`, plus each
      package's `CHANGELOG.md`.
12. **Verify**:
    - Checks: `uv run ruff check packages/` and `uv run ty check packages/`;
      `cargo fmt --check && cargo clippy --all-targets -- -D warnings`, since
      the Cargo.toml version changes; and
      `uv run pytest packages/tkati-core packages/tkati-node-el packages/tkati-node-dedup`
      against the local Redpanda and ClickHouse.
    - Manual check: run `tkati-node-el` against Redpanda with input flowing
      and send it SIGTERM. It logs the stop, exits 0 within one batch, and
      the group's committed offset matches the last batch it logged.
    - Afterwards: set the doc's `status` to `IMPLEMENTED` and add
      `## Implementation notes (as built)` and `## Verification`.

## Implementation notes (as built)

Built as specified. Where the code differs from the Design section, or makes a
decision the Design section left open:

- **Discarded reads aren't counted.** When a stop is requested during a read,
  `__next__` ends before incrementing `iterations`. The read's result was
  never handed out, so it isn't an iteration.
- **Explicit `done()` instead of the implicit commit.** The first
  implementation followed the original design: the next event request
  committed the batch, `Batch.after_commit(fn)` ran post-commit code, and
  `Node.flush` let dedup wait for delivery before marking keys seen. Review
  replaced all three with a synchronous `Batch.done()`, so the node states
  where a batch ends and writes its tail work on the lines after it. Dedup's
  order became deliver → commit → mark seen as a result (see **Nodes**).
  `test_keys_are_marked_seen_after_delivery_and_commit` pins that order.
- **`node.done(event, output=)` instead of `send` plus `Batch.done()`.** A
  second review step folded sending into finishing. The output is passed to
  the call that commits its input, `send` and `add_rows_out` are gone
  (`rows_out=` replaces the latter for nodes without a producer), and
  `Batch` became a frozen dataclass with no reference back to the node.
- **Dedup flushes every batch.** `done()` flushes even when every row was a
  duplicate and nothing was sent. The old loop skipped produce and flush in
  that case. A flush with nothing in flight returns immediately.
- **Extra fields on the test doubles.** `MemoryConsumer` also logs
  `read:<seq>`. `MemoryProducer` takes a `name`, so a DLQ double logs
  `close:dlq`.
- **Test layout.** ruff's `PT012` wants a single statement inside
  `pytest.raises`, so tests that expect an error from the loop body run the
  loop in a small named function.
- **Signal handlers.** `_restore_signal_handlers` puts back `SIG_DFL` when
  `signal.signal` reported the previous handler as `None`, meaning it wasn't
  installed from Python.
- **Version.** Released as 0.7.0.

## Verification

- `uv run ruff check packages/` and `uv run ty check packages/` are clean.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and
  `cargo test` pass. Only the Cargo.toml version changed on the Rust side.
- `uv run pytest packages/tkati-core packages/tkati-node-el packages/tkati-node-dedup packages/tkati-dashboard`
  passes against local Redpanda and ClickHouse (209 tests).
  `packages/tkati-core/tests/test_node.py` holds 36 broker-free harness
  tests, covering every behaviour listed in step 5.
- Manual check: `tkati-node-el`, Kafka to Kafka, with input flowing at about
  50k msg/s and `batch_size = 1000`, got SIGTERM six seconds into its run.
  - It logged the stop, closed the consumer and then the producer, and exited
    with code 0 about 0.1 s after the signal.
  - The group's committed input offset (56000) equalled the number of messages
    in the output topic (56000): nothing was committed without being
    delivered, and nothing delivered was left uncommitted.
  - Repeated after the switch to `done()`, and again after the switch to
    `node.done(event, output=)`: each time exit code 0 about 0.1 s after the
    signal, with 55000 committed and 55000 in the output topic.
