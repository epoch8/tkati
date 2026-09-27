"""In-memory test doubles for nodes built on `SyncNode`.

`memory_node(batches)` gives a `SyncNode` over a `MemoryConsumer` that reads the
given batches (Arrow tables or lists of dicts) and then stops the loop, and a `MemoryProducer` that records what
it is sent. Both append to a shared `log`, so a test can check the order of
operations across them:

    node, consumer, producer = memory_node([table])
    with node:
        run(node)
    assert consumer.log == ["read:0", "produce", "flush", "commit:0", ...]
"""

import threading
import time
from collections.abc import Callable, Iterable, Sized
from typing import Any, Literal

import pyarrow as pa

from tkati_core._native import BatchOffsets
from tkati_core.consumer import ConsumedBatch, Consumer
from tkati_core.node import PipelinedNode, SyncNode
from tkati_core.producer import Producer
from tkati_core.stats import LoopStats

type _Rows = pa.Table | list[dict]

# What `MemoryConsumer` reads once its batches have run out.
_EXHAUSTED: Any = object()


class MemoryConsumer(Consumer):
    """Reads `batches` in order; a None entry reads as an empty poll.

    Each entry may be an Arrow table or a list of dicts. `read_arrow` and
    `read_pylist` convert it to the format they return, so one list of
    batches serves either kind of node.

    Once `batches` runs out, every read calls `on_exhausted` (if set) and
    returns None. Commits and rewinds are recorded by batch `seq`, in
    `commits` and `rewinds`.
    """

    def __init__(
        self,
        batches: Iterable[_Rows | None],
        *,
        on_exhausted: Callable[[], object] | None = None,
        log: list[str] | None = None,
        delay: float = 0.0,
    ) -> None:
        self._batches = iter(batches)
        # Reads can come from a node's read-ahead thread while the loop
        # commits, so the shared state is updated under a lock.
        self._lock = threading.Lock()
        # Seconds each read takes, to stand in for a poll waiting on a broker.
        self.delay = delay
        self._next_seq = 0
        self.on_exhausted = on_exhausted
        self.log: list[str] = log if log is not None else []
        self.commits: list[int] = []
        self.rewinds: list[int] = []
        self.stats_seen: list[LoopStats | None] = []
        self.closed = False

    def read_arrow(
        self,
        timeout: int,
        num_messages: int,
        stats: LoopStats | None = None,
    ) -> ConsumedBatch[pa.Table] | None:
        return self._read(stats, _to_table)

    def read_pylist(
        self,
        timeout: int,
        num_messages: int,
        stats: LoopStats | None = None,
    ) -> ConsumedBatch[list[dict]] | None:
        return self._read(stats, _to_pylist)

    def _read[T: Sized](
        self, stats: LoopStats | None, convert: Callable[[_Rows], T]
    ) -> ConsumedBatch[T] | None:
        if self.delay:
            time.sleep(self.delay)
        with self._lock:
            self.stats_seen.append(stats)
            try:
                rows = next(self._batches)
            except StopIteration:
                rows = _EXHAUSTED
            if rows is None or rows is _EXHAUSTED:
                batch = None
            else:
                batch = ConsumedBatch(
                    data=convert(rows), offsets=BatchOffsets(), seq=self._next_seq
                )
                self._next_seq += 1
                self.log.append(f"read:{batch.seq}")
        if rows is _EXHAUSTED and self.on_exhausted is not None:
            self.on_exhausted()
        return batch

    def commit(self, batch: ConsumedBatch) -> None:
        with self._lock:
            self.commits.append(batch.seq)
            self.log.append(f"commit:{batch.seq}")

    def rewind(self, batch: ConsumedBatch) -> None:
        with self._lock:
            self.rewinds.append(batch.seq)
            self.log.append(f"rewind:{batch.seq}")

    def close(self) -> None:
        self.closed = True
        self.log.append("close:consumer")


class MemoryProducer(Producer):
    """Records what it is sent, in `sent`, as given: a table from
    `produce_arrow`, a list of dicts from `produce_pylist`, and each send's
    tag in `tags`. `flush` raises `fail_flush` when set, and
    `wait_delivered(tag)` raises `fail_delivery[tag]` when there is one.

    With `deliver="immediate"` (the default) everything sent counts as
    delivered at once. With `deliver="manual"` a tag's messages stay in
    flight until the test calls `release(tag)` (or `fail(tag, error)`), so it
    can hold deliveries back and release them out of order. A blocking
    `wait_delivered` then calls `on_wait(tag)` first, the point at which a
    single-threaded test can release or fail a delivery, and raises
    AssertionError if the tag is still in flight after it, rather than hang.
    A tag nothing was sent with counts as delivered, as with Kafka.
    """

    def __init__(
        self,
        *,
        fail_flush: BaseException | None = None,
        fail_delivery: dict[int, BaseException] | None = None,
        deliver: Literal["immediate", "manual"] = "immediate",
        on_wait: Callable[[int], object] | None = None,
        log: list[str] | None = None,
        name: str = "producer",
    ) -> None:
        self.fail_flush = fail_flush
        self.fail_delivery: dict[int, BaseException] = dict(fail_delivery or {})
        self.deliver = deliver
        self.on_wait = on_wait
        self.released: set[int] = set()
        self.waited_on: list[int] = []
        self.tags: list[int | None] = []
        self.log: list[str] = log if log is not None else []
        self.name = name
        self.sent: list[_Rows] = []
        self.flushes = 0
        self.stats_seen: list[LoopStats | None] = []
        self.closed = False

    def produce_arrow(
        self,
        data: pa.Table,
        stats: LoopStats | None = None,
        tag: int | None = None,
    ) -> None:
        self._record(data, stats, tag)

    def produce_pylist(
        self,
        rows: list[dict],
        stats: LoopStats | None = None,
        tag: int | None = None,
    ) -> None:
        self._record(rows, stats, tag)

    def _record(self, rows: _Rows, stats: LoopStats | None, tag: int | None) -> None:
        self.stats_seen.append(stats)
        self.sent.append(rows)
        self.tags.append(tag)
        self.log.append("produce")

    def release(self, *tags: int) -> None:
        """Deliver these tags' messages (`deliver="manual"`)."""
        self.released.update(tags)

    def fail(self, tag: int, error: BaseException) -> None:
        """Fail this tag's delivery with `error`."""
        self.fail_delivery[tag] = error

    def wait_delivered(self, tag: int, timeout: float | None = None) -> bool:
        # Logged only when it may block: a non-blocking check can run at
        # every event, and would drown the log.
        if timeout != 0:
            self.log.append(f"wait:{tag}")
        if self._in_flight(tag):
            if timeout == 0:
                return False
            self.waited_on.append(tag)
            if self.on_wait is not None:
                self.on_wait(tag)
            if self._in_flight(tag):
                raise AssertionError(
                    f"blocked on tag {tag}, which the test never releases"
                )
        if tag in self.fail_delivery:
            raise self.fail_delivery[tag]
        return True

    def _in_flight(self, tag: int) -> bool:
        return (
            self.deliver == "manual"
            and tag in self.tags
            and tag not in self.released
            and tag not in self.fail_delivery
        )

    def flush(self, stats: LoopStats | None = None) -> None:
        self.stats_seen.append(stats)
        self.flushes += 1
        self.log.append("flush")
        if self.fail_flush is not None:
            raise self.fail_flush

    def close(self) -> None:
        self.closed = True
        self.log.append(f"close:{self.name}")


def _to_table(rows: _Rows) -> pa.Table:
    return rows if isinstance(rows, pa.Table) else pa.Table.from_pylist(rows)


def _to_pylist(rows: _Rows) -> list[dict]:
    return rows.to_pylist() if isinstance(rows, pa.Table) else rows


def memory_node(
    batches: Iterable[_Rows | None],
    *,
    batch_size: int = 100,
    phases: tuple[str, ...] | None = None,
    output: bool = True,
    fail_flush: BaseException | None = None,
    fail_delivery: dict[int, BaseException] | None = None,
    read_ahead: int = 0,
    delay: float = 0.0,
) -> tuple[SyncNode, MemoryConsumer, MemoryProducer | None]:
    """A `SyncNode` over in-memory doubles that share one log. The loop ends once
    `batches` is used up. With `output=False` the node has no producer, and
    None is returned in its place."""
    log: list[str] = []
    consumer = MemoryConsumer(batches, log=log, delay=delay)
    producer = (
        MemoryProducer(fail_flush=fail_flush, fail_delivery=fail_delivery, log=log)
        if output
        else None
    )
    node = SyncNode(
        consumer,
        producer,
        batch_size=batch_size,
        batch_timeout_sec=0,
        phases=phases,
        read_ahead=read_ahead,
    )
    consumer.on_exhausted = node._end_of_input
    return node, consumer, producer


def memory_pipelined_node(
    batches: Iterable[_Rows | None],
    *,
    batch_size: int = 100,
    phases: tuple[str, ...] | None = None,
    output: bool = True,
    deliver: Literal["immediate", "manual"] = "immediate",
    max_in_flight: int = 4,
    read_ahead: int = 0,
) -> tuple[PipelinedNode, MemoryConsumer, MemoryProducer | None]:
    """`memory_node` for a `PipelinedNode`. With `deliver="manual"` the
    producer holds deliveries until the test releases them."""
    log: list[str] = []
    consumer = MemoryConsumer(batches, log=log)
    producer = MemoryProducer(deliver=deliver, log=log) if output else None
    node = PipelinedNode(
        consumer,
        producer,
        max_in_flight=max_in_flight,
        batch_size=batch_size,
        batch_timeout_sec=0,
        phases=phases,
        read_ahead=read_ahead,
    )
    consumer.on_exhausted = node._end_of_input
    return node, consumer, producer
