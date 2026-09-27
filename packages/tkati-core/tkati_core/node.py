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

import signal
import threading
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
from tkati_core.stats import LoopStats

# The phases the harness times itself (through the consumer and producer it
# passes its stats to), in report order. A node that adds its own passes the
# whole tuple, so it decides where its columns go.
DEFAULT_PHASES = (*CONSUMER_PHASES, *PRODUCER_PHASES, "commit")
# The default for a node without an output producer.
SINK_PHASES = (*CONSUMER_PHASES, "commit")

_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


# eq=False: a batch is identified by the object, which is how `Node.done`
# tells the current batch from one already finished. Comparing two batches
# by value would compare their tables.
@dataclass(frozen=True, slots=True, eq=False, repr=False)
class Batch[T: Sized]:
    """A batch read from the input. Finish it with `Node.done`.

    `data` is a `pa.Table` from `Node.consume_arrow()`, or a `list[dict]` from
    `Node.consume_pylist()`. The harness keeps the batch as read, to commit or
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


class _Stop(BaseException):
    """Raised by the signal handler to cut a blocked read short.

    A BaseException so that `except Exception` blocks in the consumer let it
    through to `Node._read`, which catches it around the read.
    """


class Node:
    """Runs a node's loop: read, hand the batch to the node, commit it when
    the node says it is done.

    Use as a context manager, and consume inside it, choosing the input
    format with `consume_arrow()` or `consume_pylist()`::

        with Node.from_settings(settings) as node:
            for event in node.consume_arrow():
                if isinstance(event, Batch):
                    node.done(event, output_arrow=transform(event.data))

    `producer` may be None, for a node that delivers its output itself (e.g.
    through a cloud API client). Such a node must have finished its writes for
    a batch before it calls `done()`, and reports them with `rows_out=`.
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
    ) -> None:
        """
        Args:
            consumer: The input. The node takes ownership and closes it.
            producer: The output, or None if the node delivers its output
                itself. Closed by the node.
            batch_size: Messages to read per batch at most.
            batch_timeout_sec: How long a read waits for a full batch.
            phases: The perf report's columns, in order. Must include every
                phase the harness times: `CONSUMER_PHASES`, `commit`, and
                `PRODUCER_PHASES` when there is a producer. Defaults to
                `DEFAULT_PHASES`, or `SINK_PHASES` without a producer.
            dlq: The producer `producer` routes rejected rows to, if any. Only
                closed here, after `producer`.
            metrics: Where to serve `/metrics`, started on entering the node.
                None serves nothing.
            handle_signals: Turn SIGTERM and SIGINT into a stop after the
                current batch (a second signal forces one).
            stop_when_idle: End the loop at the first empty poll instead of
                yielding `Idle` — for tests and one-shot runs that should
                process what is in the input and exit.
        """
        required = (
            *CONSUMER_PHASES,
            *(PRODUCER_PHASES if producer is not None else ()),
            "commit",
        )
        if phases is None:
            phases = DEFAULT_PHASES if producer is not None else SINK_PHASES
        missing = [name for name in required if name not in phases]
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
        self._stats = LoopStats(phases=phases)

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
            )
            # Built without error: the node owns them now.
            built.pop_all()
        return node

    @property
    def stats(self) -> LoopStats:
        return self._stats

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
        try:
            # A failed batch is rewound so it is read again. Not for a bare
            # BaseException (Ctrl-C twice): a forced stop shouldn't wait on a
            # seek, and the batch is uncommitted either way.
            if self._batch is not None and isinstance(exc, Exception):
                try:
                    self._consumer.rewind(self._batch)
                except Exception:
                    logger.exception("Could not rewind the failed batch")
            self._batch = None
            self._event = None
        finally:
            self._restore_signal_handlers()
            # Consumer, then output, then DLQ: the output can still route rows
            # to the DLQ. ExitStack runs callbacks last-in first-out, and runs
            # the rest even if one raises.
            with ExitStack() as closing:
                if self._dlq is not None:
                    closing.callback(self._dlq.close)
                if self._producer is not None:
                    closing.callback(self._producer.close)
                closing.callback(self._consumer.close)

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
            raise RuntimeError("consume from a Node inside `with node:`")
        return self._events(read)

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
            raise RuntimeError("consume from a Node inside `with node:`")

        if self._batch is not None:
            # Raised out of the node's `for` loop, so `__exit__` rewinds the
            # batch: nothing is lost, and the bug is loud instead of showing up
            # as consumer lag.
            raise RuntimeError(
                f"batch {self._batch.seq} was not marked done before the next "
                "event was requested; call `node.done(event)` once the batch is finished"
            )

        self._stats.report_if_due()

        if self._stop_requested:
            return None

        consumed = self._read(read)
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
        self._stats.rows_in += len(data)

        event = Batch(data, short)
        self._batch = consumed
        self._event = event
        return event

    def _read[T: Sized](
        self, read: Callable[..., ConsumedBatch[T] | None]
    ) -> ConsumedBatch[T] | None:
        # No phase block here: the consumer splits its own time into `poll`
        # and `parse`. Wrapping it in an umbrella phase as well would
        # double-count that time.
        try:
            try:
                self._reading = True
                return read(
                    timeout=self._batch_timeout_sec,
                    num_messages=self._batch_size,
                    stats=self._stats,
                )
            finally:
                self._reading = False
        # Outside the inner try, so a _Stop raised as `_reading` is being
        # cleared is caught too.
        except _Stop:
            return None

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
                    self._producer.produce_arrow(output_arrow, stats=self._stats)
            elif output_pylist is not None:
                rows_out = len(output_pylist)
                if rows_out > 0:
                    self._producer.produce_pylist(output_pylist, stats=self._stats)

        if self._producer is not None:
            # KafkaProducer.produce_arrow() only enqueues: block until
            # delivered before committing, or a crash loses the batch while
            # its offset says it was handled. A no-op for ClickhouseProducer,
            # whose inserts are synchronous.
            self._producer.flush(stats=self._stats)
        with self._stats.phase("commit"):
            self._consumer.commit(batch)

        # Committed: nothing left to rewind, whatever the node does next.
        self._batch = None
        self._event = None
        # Counted only once committed, so a failed batch isn't counted.
        self._stats.rows_out += rows_out or 0

    def phase(self, name: str) -> AbstractContextManager[None]:
        """Time a block of the node's own work into phase `name`."""
        return self._stats.phase(name)

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
