"""In-memory test doubles for nodes built on `Node`.

`memory_node(batches)` gives a `Node` over a `MemoryConsumer` that reads the
given batches (Arrow tables or lists of dicts) and then stops the loop, and a `MemoryProducer` that records what
it is sent. Both append to a shared `log`, so a test can check the order of
operations across them:

    node, consumer, producer = memory_node([table])
    with node:
        run(node)
    assert consumer.log == ["read:0", "produce", "flush", "commit:0", ...]
"""

from collections.abc import Callable, Iterable, Sized

import pyarrow as pa

from tkati_core._native import BatchOffsets
from tkati_core.consumer import ConsumedBatch, Consumer
from tkati_core.node import Node
from tkati_core.producer import Producer
from tkati_core.stats import LoopStats

type _Rows = pa.Table | list[dict]


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
    ) -> None:
        self._batches = iter(batches)
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
        self.stats_seen.append(stats)
        try:
            rows = next(self._batches)
        except StopIteration:
            if self.on_exhausted is not None:
                self.on_exhausted()
            return None
        if rows is None:
            return None
        batch = ConsumedBatch(
            data=convert(rows), offsets=BatchOffsets(), seq=self._next_seq
        )
        self._next_seq += 1
        self.log.append(f"read:{batch.seq}")
        return batch

    def commit(self, batch: ConsumedBatch) -> None:
        self.commits.append(batch.seq)
        self.log.append(f"commit:{batch.seq}")

    def rewind(self, batch: ConsumedBatch) -> None:
        self.rewinds.append(batch.seq)
        self.log.append(f"rewind:{batch.seq}")

    def close(self) -> None:
        self.closed = True
        self.log.append("close:consumer")


class MemoryProducer(Producer):
    """Records what it is sent, in `sent`, as given: a table from
    `produce_arrow`, a list of dicts from `produce_pylist`. `flush` raises
    `fail_flush` when set."""

    def __init__(
        self,
        *,
        fail_flush: BaseException | None = None,
        log: list[str] | None = None,
        name: str = "producer",
    ) -> None:
        self.fail_flush = fail_flush
        self.log: list[str] = log if log is not None else []
        self.name = name
        self.sent: list[_Rows] = []
        self.flushes = 0
        self.stats_seen: list[LoopStats | None] = []
        self.closed = False

    def produce_arrow(self, data: pa.Table, stats: LoopStats | None = None) -> None:
        self.stats_seen.append(stats)
        self.sent.append(data)
        self.log.append("produce")

    def produce_pylist(self, rows: list[dict], stats: LoopStats | None = None) -> None:
        self.stats_seen.append(stats)
        self.sent.append(rows)
        self.log.append("produce")

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
) -> tuple[Node, MemoryConsumer, MemoryProducer | None]:
    """A `Node` over in-memory doubles that share one log. The loop ends once
    `batches` is used up. With `output=False` the node has no producer, and
    None is returned in its place."""
    log: list[str] = []
    consumer = MemoryConsumer(batches, log=log)
    producer = MemoryProducer(fail_flush=fail_flush, log=log) if output else None
    node = Node(
        consumer,
        producer,
        batch_size=batch_size,
        batch_timeout_sec=0,
        phases=phases,
    )
    consumer.on_exhausted = node.stop
    return node, consumer, producer
