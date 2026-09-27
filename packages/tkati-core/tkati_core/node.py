"""The worker loop harness: a node's input, output, stats and lifecycle behind
a `for event in node.consume_arrow():` (or `consume_pylist()`) loop.

The node writes what happens to one batch; the harness does everything around
it. The one rule to know:

    A node finishes each batch with `node.done(event, output_arrow=...)`.

`done()` sends the batch's output, waits until it is delivered, commits the
batch, and returns once the commit is made. Work that must only happen after
the commit goes on the lines after it. Asking for the next event without
having called `done()` is a bug, and raises.

A batch that isn't done is left uncommitted, so it is read again on restart:

- an exception rewinds it (`Consumer.rewind`) and propagates;
- `break` or `KeyboardInterrupt` just leaves it, without waiting on a seek.

`done()` being synchronous is a step on the way to a pipelined loop, where it
will return before delivery and take callbacks for the work that follows.
Until then, code after `done()` runs after the commit.

Design doc: design-docs/2026-09-26-worker-loop-harness.md.
"""

import queue
import signal
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator, Sized
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from types import FrameType, TracebackType
from typing import Any, Self

import pyarrow as pa
from loguru import logger

from tkati_core.consumer import (
    CONSUMER_PHASES,
    ConsumedBatch,
    Consumer,
    build_consumer,
)
from tkati_core.metrics import MetricsSettings, start_metrics_server
from tkati_core.producer import PRODUCER_PHASES, Producer, build_producer
from tkati_core.settings import NodeSettings
from tkati_core.stats import LoopStats, PhaseStats

# The phases the harness times on the loop thread (itself, and through the
# producer it passes its stats to), in report order: the order they happen in.
# A node that adds its own passes the whole tuple, so it decides where its
# columns go. The consumer's phases aren't here: reading is timed apart, in
# the node's `read_stats`, and reported on a line of its own.
DEFAULT_PHASES = ("wait/input", *PRODUCER_PHASES, "commit")
# The default for a node without an output producer.
SINK_PHASES = ("wait/input", "commit")
# PipelinedNode's default: it also times `done()` waiting for a free slot.
PIPELINED_PHASES = ("wait/input", *PRODUCER_PHASES, "wait/in-flight", "commit")

_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)

# How long the loop and the read-ahead thread block on the queue between
# checks for a stop, as the native poll's POLL_STEP does.
_WAIT_STEP_SEC = 0.1


# eq=False: a batch is identified by the object, which is how `done()`
# tells the current batch from one already finished. Comparing two batches
# by value would compare their tables.
@dataclass(frozen=True, slots=True, eq=False, repr=False)
class Batch[T: Sized]:
    """A batch read from the input. Finish it with the node's `done()`.

    `data` is a `pa.Table` from `consume_arrow()`, or a `list[dict]` from
    `consume_pylist()`. The harness keeps the batch as read, to commit or
    rewind; the node only sees its data, so filtering or transforming `data`
    never changes what is committed.
    """

    data: T
    # Fewer rows than the batch size: the poll drained the input and waited
    # out the batch timeout.
    short: bool

    def __repr__(self) -> str:
        return f"Batch({len(self.data)} rows, short={self.short})"


class Idle:
    """A poll returned nothing."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "Idle()"


type Event[T: Sized] = Batch[T] | Idle


@dataclass(frozen=True, slots=True)
class _ReadFailed:
    """A read on the read-ahead thread raised: handed to the loop to raise."""

    error: BaseException


# Handed over by the read-ahead thread after the read that ended the input.
_END_OF_INPUT: Any = object()


def _tag(batch: ConsumedBatch[Any]) -> int:
    """The delivery tag for a batch's output. 0 means untracked, hence +1."""
    return batch.seq + 1


class _Stop(BaseException):
    """Raised by the signal handler to cut a blocked read short.

    A BaseException so that `except Exception` blocks in the consumer let it
    through to `_NodeBase._read`, which catches it around the read.
    """


class _NodeBase:
    """What the node classes share: building and closing the input and
    output, consuming, stats, signals and stop. Each subclass adds its own
    `done()`, which is where they differ.

    Deliberately private, and neither subclass derives from the other: code
    written against `SyncNode.done()` relies on the batch being committed when
    it returns, so it must not accept a node for which that isn't true.
    """

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
        read_ahead: int = 0,
    ) -> None:
        """
        Args:
            consumer: The input. The node takes ownership and closes it.
            producer: The output, or None if the node delivers its output
                itself. Closed by the node.
            batch_size: Messages to read per batch at most.
            batch_timeout_sec: How long a read waits for a full batch.
            phases: The perf report's loop line, in order: what the loop
                thread spends its time on. Must include every phase the
                harness times there: `wait/input`, `commit`, and
                `PRODUCER_PHASES` when there is a producer. Defaults to
                `DEFAULT_PHASES`, or `SINK_PHASES` without a producer. Must
                not include `CONSUMER_PHASES`: reading is timed apart, in
                `read_stats`.
            dlq: The producer `producer` routes rejected rows to, if any. Only
                closed here, after `producer`.
            metrics: Where to serve `/metrics`, started on entering the node.
                None serves nothing.
            handle_signals: Turn SIGTERM and SIGINT into a stop after the
                current batch (a second signal forces one).
            read_ahead: Batches to read in the background, on a thread of the
                node's own, while the loop body processes the current one.
                0 reads on the loop thread when the next event is asked for.
                `from_settings` takes it from `[pipeline] read_ahead`
                (default 1).
            stop_when_idle: End the loop at the first empty poll instead of
                yielding `Idle` — for tests and one-shot runs that should
                process what is in the input and exit.
        """
        if read_ahead < 0:
            raise ValueError(
                "read_ahead must be 0 (off) or a positive number of batches"
            )
        required = (
            "wait/input",
            *(PRODUCER_PHASES if producer is not None else ()),
            *self._EXTRA_PHASES,
            "commit",
        )
        if phases is None:
            phases = self._default_phases(producer is not None)
        missing = [name for name in required if name not in phases]
        misplaced = [name for name in CONSUMER_PHASES if name in phases]
        if misplaced:
            raise ValueError(
                f"phases includes {misplaced}, which are timed in `read_stats` "
                "and reported on the read line; leave them out"
            )
        if missing:
            # A column the harness fills in but the report doesn't show would
            # hide real time, so refuse rather than silently drop it.
            raise ValueError(f"phases is missing {missing}, which the harness times")

        self._consumer = consumer
        self._producer = producer
        self._dlq = dlq
        self._batch_size = batch_size
        self._batch_timeout_sec = batch_timeout_sec
        self._metrics = metrics
        self._handle_signals = handle_signals
        self._stop_when_idle = stop_when_idle
        self._read_ahead = read_ahead
        # The read-ahead thread, started by the first consume_*() call, the
        # read method it calls, and what it hands the loop: a ConsumedBatch, a
        # None for an empty poll, or a _ReadFailed.
        self._reader: threading.Thread | None = None
        self._reader_read: Callable[..., Any] | None = None
        self._reader_stop = threading.Event()
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=max(read_ahead, 1))
        # Set by `_end_of_input()` during a read that found the input used up.
        self._input_ended = False
        self._stats = LoopStats(phases=phases, label="loop")
        # Reading, timed apart from the loop: with read-ahead it runs on a
        # thread of its own, and even without, its poll and parse are what
        # the loop's `wait/input` is made of. With read-ahead it also times
        # the reader waiting for the loop to take what it read. Reported with
        # the loop's stats, but not exported as metrics.
        self._read_stats = PhaseStats(
            phases=(*CONSUMER_PHASES, "wait/loop") if read_ahead else CONSUMER_PHASES,
            label="read",
        )

        self._entered = False
        # The batch handed out last and not yet done, and the event the node
        # got for it.
        self._batch: ConsumedBatch[Any] | None = None
        self._event: Batch[Any] | None = None
        self._stop_requested = False
        self._reading = False
        self._signals_received = 0
        self._previous_handlers: dict[
            signal.Signals, signal.Handlers | Callable | int | None
        ] = {}

    @classmethod
    def from_settings(
        cls, settings: NodeSettings, *, phases: tuple[str, ...] | None = None
    ) -> Self:
        """Build the input, output and DLQ from settings, with metrics and
        signal handling on. The output and DLQ are built only when
        `settings.output` is set."""
        with ExitStack() as built:
            producer: Producer | None = None
            dlq: Producer | None = None
            if settings.output is not None:
                if settings.dlq is not None:
                    dlq = build_producer(settings.dlq)
                    built.callback(dlq.close)
                producer = build_producer(settings.output, dlq_producer=dlq)
                built.callback(producer.close)
            consumer = build_consumer(settings.input)
            built.callback(consumer.close)

            node = cls(
                consumer,
                producer,
                batch_size=settings.input.consumer.batch_size,
                batch_timeout_sec=settings.input.consumer.batch_timeout_sec,
                phases=phases,
                dlq=dlq,
                metrics=settings.metrics,
                handle_signals=True,
                read_ahead=settings.pipeline.read_ahead,
                **cls._settings_kwargs(settings),
            )
            # Built without error: the node owns them now.
            built.pop_all()
        return node

    @property
    def stats(self) -> LoopStats:
        """The loop thread's stats, and the loop's counts."""
        return self._stats

    @property
    def read_stats(self) -> PhaseStats:
        """Reading's stats: `CONSUMER_PHASES`, and `wait/loop` with
        read-ahead."""
        return self._read_stats

    def __enter__(self) -> Self:
        if self._metrics is not None:
            # Same numbers as the periodic perf log line, as Prometheus
            # counters.
            start_metrics_server(self._metrics, self._stats)
        if self._handle_signals:
            self._install_signal_handlers()
        self._entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._entered = False
        # A clean exit (the loop ended, or the node broke out of it) finishes
        # what the node finished: PipelinedNode commits batches still waiting
        # for delivery. A failure there fails the exit like any other.
        failure = exc
        drain_error: Exception | None = None
        try:
            if exc is None:
                # Inside the try, so a forced stop while draining (a second
                # Ctrl-C) still closes everything.
                try:
                    self._drain()
                except Exception as error:
                    drain_error = failure = error
            # A failed batch is rewound so it is read again. Not for a bare
            # BaseException (Ctrl-C twice): a forced stop shouldn't wait on a
            # seek, and the batch is uncommitted either way.
            oldest = self._oldest_uncommitted()
            if oldest is not None and isinstance(failure, Exception):
                # The read-ahead thread must not be reading while we seek: a
                # read in progress could hand out messages fetched before the
                # seek, under a sequence number that could then be committed.
                if self._stop_reader(timeout=self._batch_timeout_sec + 1):
                    try:
                        self._consumer.rewind(oldest)
                    except Exception:
                        logger.exception("Could not rewind the failed batch")
                else:
                    # Uncommitted either way, so it is read again after a
                    # restart; the rewind only matters if the process lived on.
                    logger.warning(
                        "Read-ahead thread didn't stop in time; not rewinding the failed batch"
                    )
            self._batch = None
            self._event = None
        finally:
            self._restore_signal_handlers()
            # Told to stop before the consumer closes, so the error its read
            # then gets reads as the end, not as a failure to hand on.
            self._reader_stop.set()
            # Consumer, then output, then DLQ: the output can still route rows
            # to the DLQ. ExitStack runs callbacks last-in first-out, and runs
            # the rest even if one raises. The read-ahead thread is joined
            # last: closing the consumer ends its poll within one POLL_STEP.
            with ExitStack() as closing:
                closing.callback(self._stop_reader, timeout=self._batch_timeout_sec + 1)
                if self._dlq is not None:
                    closing.callback(self._dlq.close)
                if self._producer is not None:
                    closing.callback(self._producer.close)
                closing.callback(self._consumer.close)
        if drain_error is not None:
            raise drain_error

    def consume_arrow(self) -> Iterator[Batch[pa.Table] | Idle]:
        """Iterate the input as Arrow tables (`Consumer.read_arrow`). A
        message that fails to decode fails its whole batch."""
        return self._consume(self._consumer.read_arrow)

    def consume_pylist(self) -> Iterator[Batch[list[dict]] | Idle]:
        """Iterate the input as lists of dicts (`Consumer.read_pylist`). A
        message that fails to decode is logged and skipped, so a batch can be
        shorter than what was read."""
        return self._consume(self._consumer.read_pylist)

    def _consume[T: Sized](
        self, read: Callable[..., ConsumedBatch[T] | None]
    ) -> Iterator[Batch[T] | Idle]:
        # Checked here rather than inside the generator, so misuse raises at
        # the call, not at the first iteration.
        if not self._entered:
            raise RuntimeError("consume from a node inside `with node:`")
        if self._read_ahead > 0:
            if self._reader is None:
                self._start_reader(read)
            elif read != self._reader_read:
                raise RuntimeError(
                    "this node is already reading ahead in the other format"
                )
        return self._events(read)

    def _start_reader(self, read: Callable[..., Any]) -> None:
        self._reader_read = read
        self._reader = threading.Thread(
            target=self._read_ahead_loop,
            args=(read,),
            name="tkati-read-ahead",
            daemon=True,
        )
        self._reader.start()

    def _read_ahead_loop(self, read: Callable[..., Any]) -> None:
        """The read-ahead thread: read, hand over, repeat, until stopped. A
        read that fails is handed over too, for the loop to raise, and ends
        the thread. It doesn't catch signals: they are only ever handled on
        the main thread."""
        while not self._reader_stop.is_set():
            try:
                item = read(
                    timeout=self._batch_timeout_sec,
                    num_messages=self._batch_size,
                    stats=self._read_stats,
                )
            except BaseException as error:
                if not self._reader_stop.is_set():
                    self._hand_over(_ReadFailed(error))
                return
            if self._input_ended:
                # After everything read before it, so nothing is dropped.
                self._hand_over(_END_OF_INPUT)
                return
            if not self._hand_over(item):
                return

    def _hand_over(self, item: Any) -> bool:
        """Put `item` on the queue, waiting for room; False if stopped first.
        Time spent waiting is the reader held up by the loop, recorded as
        `wait/loop`."""
        started = time.perf_counter()
        try:
            while not self._reader_stop.is_set():
                try:
                    self._queue.put(item, timeout=_WAIT_STEP_SEC)
                except queue.Full:
                    continue
                return True
            return False
        finally:
            self._read_stats.record("wait/loop", time.perf_counter() - started)

    def _take(self) -> ConsumedBatch[Any] | None:
        """The next read from the read-ahead thread: a batch, or None for an
        empty poll or once a stop is requested. Time spent waiting is the
        loop being starved of input, recorded as `wait/input`."""
        started = time.perf_counter()
        try:
            while not self._stop_requested:
                try:
                    item = self._queue.get(timeout=_WAIT_STEP_SEC)
                except queue.Empty:
                    continue
                if isinstance(item, _ReadFailed):
                    raise item.error
                if item is _END_OF_INPUT:
                    self._stop_requested = True
                    return None
                return item
            return None
        finally:
            self._stats.record("wait/input", time.perf_counter() - started)

    def _stop_reader(self, timeout: float) -> bool:
        """Stop the read-ahead thread and drop what it had read, none of which
        was handed out, so none of it is committed. True once it has
        stopped."""
        self._reader_stop.set()
        if self._reader is not None:
            self._reader.join(timeout)
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
        return self._reader is None or not self._reader.is_alive()

    def _events[T: Sized](
        self, read: Callable[..., ConsumedBatch[T] | None]
    ) -> Iterator[Batch[T] | Idle]:
        # No try/finally here: a node that breaks out of its loop leaves this
        # generator suspended, and there is nothing for it to clean up.
        while (event := self._next_event(read)) is not None:
            yield event

    def _next_event[T: Sized](
        self, read: Callable[..., ConsumedBatch[T] | None]
    ) -> Batch[T] | Idle | None:
        """One step of the loop: the next event, or None to end it."""
        if not self._entered:
            raise RuntimeError("consume from a node inside `with node:`")

        if self._batch is not None:
            # Raised out of the node's `for` loop, so `__exit__` rewinds the
            # batch: nothing is lost, and the bug is loud instead of showing up
            # as consumer lag.
            raise RuntimeError(
                f"batch {self._batch.seq} was not marked done before the next "
                "event was requested; call `node.done(event)` once the batch is finished"
            )

        self._before_read()
        if self._stats.report_if_due():
            self._read_stats.report()

        if self._stop_requested:
            return None

        if self._reader is not None:
            consumed = self._take()
        else:
            # Without read-ahead the loop waits on the read itself.
            with self._stats.phase("wait/input"):
                consumed = self._read(read)
        if self._input_ended and self._reader is None:
            self._stop_requested = True
        # Requested during the read — by a signal, or by `stop()`. Whatever
        # was read is left uncommitted, to be read again after restart.
        if self._stop_requested:
            return None

        self._stats.iterations += 1
        if consumed is None:
            self._stats.starved_iterations += 1
            if self._stop_when_idle:
                self._stop_requested = True
                return None
            return Idle()

        data = consumed.data
        # A short batch means the node drained the input and waited out the
        # batch timeout — it wasn't CPU-bound, so its timings say nothing
        # about whether this node can keep up. For `read_pylist` this counts
        # parsed rows, not messages read: messages that failed to decode make
        # a full poll look short.
        short = len(data) < self._batch_size
        if short:
            self._stats.starved_iterations += 1

        event = Batch(data, short)
        self._batch = consumed
        self._event = event
        return event

    def _read[T: Sized](
        self, read: Callable[..., ConsumedBatch[T] | None]
    ) -> ConsumedBatch[T] | None:
        # Timed by the caller as the loop's `wait/input`. The consumer splits
        # the same time into `poll` and `parse` in `read_stats`.
        try:
            try:
                self._reading = True
                return read(
                    timeout=self._batch_timeout_sec,
                    num_messages=self._batch_size,
                    stats=self._read_stats,
                )
            finally:
                self._reading = False
        # Outside the inner try, so a _Stop raised as `_reading` is being
        # cleared is caught too.
        except _Stop:
            return None

    def _take_batch(
        self,
        event: Batch[Any],
        output_arrow: pa.Table | None,
        output_pylist: list[dict] | None,
        rows_out: int | None,
    ) -> tuple[ConsumedBatch[Any], int]:
        """The first half of `done()`, the same in both node classes: check
        the arguments and that `event` is the current batch, then send its
        output, tagged with `_tag(batch)`. Returns the batch as read and the
        rows out. Leaves the batch outstanding, so a failure from here on
        still rewinds it."""
        given = [output_arrow, output_pylist, rows_out]
        if sum(arg is not None for arg in given) > 1:
            raise ValueError(
                "pass at most one of `output_arrow`, `output_pylist` and "
                "`rows_out`: sent rows are counted already"
            )
        batch = self._batch
        if batch is None or event is not self._event:
            raise RuntimeError(
                "done() was already called for this batch, or it isn't the current one"
            )

        if output_arrow is not None or output_pylist is not None:
            if self._producer is None:
                raise RuntimeError(
                    "this node has no output producer to send output to; "
                    "report rows delivered by the node itself with `rows_out=`"
                )
            # No phase blocks: the producer splits its own time into
            # `serialize` and `enqueue`.
            if output_arrow is not None:
                rows_out = len(output_arrow)
                if rows_out > 0:
                    self._producer.produce_arrow(
                        output_arrow, stats=self._stats, tag=_tag(batch)
                    )
            elif output_pylist is not None:
                rows_out = len(output_pylist)
                if rows_out > 0:
                    self._producer.produce_pylist(
                        output_pylist, stats=self._stats, tag=_tag(batch)
                    )
        return batch, rows_out or 0

    def _commit(self, batch: ConsumedBatch[Any], rows_out: int) -> None:
        with self._stats.phase("commit"):
            self._consumer.commit(batch)
        # Counted only once committed, so a failed batch isn't counted (nor
        # counted twice once re-read). Rows in are counted here too, not at
        # read: a `PipelinedNode` commits batches later than it reads them,
        # and a report between the two would otherwise put a batch's rows in
        # and rows out in different intervals, making "dropped" meaningless.
        # For `read_pylist` this counts parsed rows, not messages read.
        self._stats.rows_in += len(batch.data)
        self._stats.rows_out += rows_out

    def _oldest_uncommitted(self) -> ConsumedBatch[Any] | None:
        """The batch `__exit__` rewinds when the loop fails: the oldest one
        not yet committed."""
        return self._batch

    # Hooks for the subclasses; the defaults are SyncNode's.

    # Phases the subclass times itself, required in any `phases=` tuple.
    _EXTRA_PHASES: tuple[str, ...] = ()

    def _default_phases(self, has_producer: bool) -> tuple[str, ...]:
        return DEFAULT_PHASES if has_producer else SINK_PHASES

    @classmethod
    def _settings_kwargs(cls, settings: NodeSettings) -> dict[str, Any]:
        """Extra constructor arguments `from_settings` reads from settings."""
        return {}

    def _before_read(self) -> None:
        """Runs at every event request, before the next read."""

    def _drain(self) -> None:
        """Runs when the loop exits cleanly, before anything is closed."""

    def phase(self, name: str) -> AbstractContextManager[None]:
        """Time a block of the node's own work into phase `name`."""
        return self._stats.phase(name)

    def _end_of_input(self) -> None:
        """For an input that knows it is finished, such as the in-memory one
        in `tkati_core.testing`: call it from inside the read that found
        nothing left. Unlike `stop()`, which drops batches read ahead, the
        loop ends only after handing out everything read before that point."""
        self._input_ended = True

    def stop(self) -> None:
        """End the loop at the next event request. The current batch still
        has to be finished with `done()` first."""
        self._stop_requested = True

    def _install_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            logger.warning(
                "Node not on the main thread: SIGTERM/SIGINT keep their default handling"
            )
            return
        for sig in _STOP_SIGNALS:
            self._previous_handlers[sig] = signal.signal(sig, self._on_signal)

    def _restore_signal_handlers(self) -> None:
        for sig, handler in self._previous_handlers.items():
            # None means the handler wasn't installed from Python; the closest
            # we can put back is the default.
            signal.signal(sig, handler if handler is not None else signal.SIG_DFL)
        self._previous_handlers.clear()

    def _on_signal(self, signum: int, frame: FrameType | None) -> None:
        self._signals_received += 1
        if self._signals_received > 1:
            raise KeyboardInterrupt
        logger.info(
            f"{signal.Signals(signum).name} received: stopping after the current "
            "batch (send it again to force)"
        )
        self.stop()
        # A read can block for the whole batch timeout. The native poll runs
        # this handler every 100ms, so raising here cuts it short.
        if self._reading:
            raise _Stop


class SyncNode(_NodeBase):
    """Runs a node's loop: read, hand the batch to the node, and commit it
    when the node says it is done. `done()` returns once the commit is made.

    Use as a context manager, and consume inside it, choosing the input
    format with `consume_arrow()` or `consume_pylist()`::

        with SyncNode.from_settings(settings) as node:
            for event in node.consume_arrow():
                if isinstance(event, Batch):
                    node.done(event, output_arrow=transform(event.data))

    `producer` may be None, for a node that delivers its output itself (e.g.
    through a cloud API client). Such a node must have finished its writes for
    a batch before it calls `done()`, and reports them with `rows_out=`.
    """

    def done(
        self,
        event: Batch[Any],
        *,
        output_arrow: pa.Table | None = None,
        output_pylist: list[dict] | None = None,
        rows_out: int | None = None,
    ) -> None:
        """Finish `event`'s batch: send its output, wait until it is delivered,
        commit the batch as read, and count the rows out. Returns once the
        commit is made, so the lines after it run after the commit.

        Pass at most one of the three keyword arguments. The output format is
        named explicitly and needn't match the input's: a `consume_pylist()`
        node may send `output_arrow=`.

        Args:
            event: The batch being finished: the one handed out last.
            output_arrow: What the batch produced, as an Arrow table, sent with
                `Producer.produce_arrow`.
            output_pylist: What the batch produced, as a list of dicts, sent
                with `Producer.produce_pylist`.
            rows_out: For a node without a producer: how many rows it
                delivered itself for this batch. Counted toward `rows_out`
                like sent rows, so the perf line's "dropped" stays meaningful.

        None or an empty output sends nothing, e.g. when every row was
        filtered out.

        Raises RuntimeError if `event` isn't the unfinished current batch
        (e.g. `done()` twice), or if an output is given without a producer.
        If sending, flushing or committing fails, the batch stays unfinished
        and is rewound when the loop exits.
        """
        batch, rows = self._take_batch(event, output_arrow, output_pylist, rows_out)
        if self._producer is not None:
            # KafkaProducer.produce_arrow() only enqueues: block until
            # delivered before committing, or a crash loses the batch while
            # its offset says it was handled. A no-op for ClickhouseProducer,
            # whose inserts are synchronous.
            self._producer.flush(stats=self._stats)
            # flush() returns once nothing is in flight, delivered or not.
            # This raises DeliveryError if any of the batch's messages failed,
            # so the batch is rewound rather than committed. It doesn't wait:
            # after the flush every report is in.
            self._producer.wait_delivered(_tag(batch), timeout=None)
        self._commit(batch, rows)
        # Committed: nothing left to rewind, whatever the node does next.
        self._batch = None
        self._event = None


@dataclass(slots=True)
class _Finished:
    """A batch `PipelinedNode.done()` was called for, not yet committed."""

    batch: ConsumedBatch[Any]
    tag: int
    rows_out: int
    after_commit: Callable[[], object] | None


class PipelinedNode(_NodeBase):
    """Runs a node's loop like `SyncNode`, except that `done()` returns
    without waiting for delivery: the node goes on to the next batch while
    this one's output is still in flight.

    The batch is committed later, on the loop thread, once its output and
    every earlier batch's output is delivered, in read order. So the lines
    after `done()` run *before* the commit. Work that must follow the commit
    (marking keys seen, counting what a batch dropped) goes in
    `done(..., after_commit=fn)` instead. The stats' rows in and out are
    counted at the commit too, so both lag the read by the batches in flight.

    Use as a context manager, and consume inside it::

        with PipelinedNode.from_settings(settings) as node:
            for event in node.consume_arrow():
                if isinstance(event, Batch):
                    node.done(event, output_arrow=transform(event.data))

    Deliberately not a subclass of `SyncNode`: code written for
    `SyncNode.done()` relies on the commit having happened when it returns.
    """

    _EXTRA_PHASES = ("wait/in-flight",)

    def __init__(
        self,
        consumer: Consumer,
        producer: Producer | None,
        *,
        max_in_flight: int = 4,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            max_in_flight: Batches `done()` may leave waiting for delivery.
                When there are more, `done()` blocks until the oldest one is
                delivered and committed. `from_settings` takes it from
                `[pipeline] max_in_flight` (default 4).
            **kwargs: As for `SyncNode`.
        """
        if max_in_flight < 1:
            raise ValueError("max_in_flight must be at least 1 batch")
        super().__init__(consumer, producer, **kwargs)
        self._max_in_flight = max_in_flight
        self._finished: deque[_Finished] = deque()

    def _default_phases(self, has_producer: bool) -> tuple[str, ...]:
        if has_producer:
            return PIPELINED_PHASES
        return ("wait/input", "wait/in-flight", "commit")

    @classmethod
    def _settings_kwargs(cls, settings: NodeSettings) -> dict[str, Any]:
        return {"max_in_flight": settings.pipeline.max_in_flight}

    def done(
        self,
        event: Batch[Any],
        *,
        output_arrow: pa.Table | None = None,
        output_pylist: list[dict] | None = None,
        rows_out: int | None = None,
        after_commit: Callable[[], object] | None = None,
    ) -> None:
        """Finish `event`'s batch: send its output and return, without waiting
        for delivery (unless `max_in_flight` batches are already waiting, in
        which case wait for the oldest first).

        The batch is committed once its output and every earlier batch's
        output is delivered; then `after_commit` runs, on the loop thread,
        between events or inside a later `done()`. It never runs for a batch
        that isn't committed. The output arguments are as for
        `SyncNode.done()`.

        If a delivery fails, `DeliveryError` is raised from a later `done()`
        or event request, and the oldest uncommitted batch is rewound when the
        loop exits.
        """
        batch, rows = self._take_batch(event, output_arrow, output_pylist, rows_out)
        self._finished.append(_Finished(batch, _tag(batch), rows, after_commit))
        # Finished: the node may ask for the next event now.
        self._batch = None
        self._event = None
        self._settle(keep=self._max_in_flight)

    def _settle(self, keep: int) -> None:
        """Commit finished batches from the oldest while they are delivered,
        and wait for deliveries while more than `keep` are left. Stops at the
        first batch not yet delivered: commits never skip ahead."""
        while self._finished:
            head = self._finished[0]
            if not self._delivered(head.tag, wait=len(self._finished) > keep):
                return
            self._commit(head.batch, head.rows_out)
            # Popped only once committed, so a failed commit leaves it the
            # oldest uncommitted batch, the one `__exit__` rewinds.
            self._finished.popleft()
            if head.after_commit is not None:
                head.after_commit()

    def _delivered(self, tag: int, wait: bool) -> bool:
        if self._producer is None:
            return True
        if not wait:
            return self._producer.wait_delivered(tag, timeout=0)
        with self._stats.phase("wait/in-flight"):
            return self._producer.wait_delivered(tag, timeout=None)

    def _before_read(self) -> None:
        # Commit what has been delivered since the last event, without waiting.
        self._settle(keep=self._max_in_flight)

    def _drain(self) -> None:
        self._settle(keep=0)

    def _oldest_uncommitted(self) -> ConsumedBatch[Any] | None:
        if self._finished:
            return self._finished[0].batch
        return self._batch
