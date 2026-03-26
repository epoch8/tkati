"""Base classes for tkati node handlers."""

from __future__ import annotations

import pyarrow as pa


class OneToOne:
    """Single input topic, single output topic.

    Override ``process`` to transform a ``pa.RecordBatch`` and return the
    result.  Return ``None`` for sink nodes that produce no output.
    """

    def __init__(self, config: dict) -> None:  # noqa: D107
        pass

    def process(self, batch: pa.RecordBatch) -> pa.RecordBatch | None:
        raise NotImplementedError


class OneToMany:
    """Single input topic, multiple output topics.

    Override ``process`` and return a ``dict`` keyed by bare output topic
    name.  Topics absent from the dict receive no records for that batch.
    """

    def __init__(self, config: dict) -> None:  # noqa: D107
        pass

    def process(self, batch: pa.RecordBatch) -> dict[str, pa.RecordBatch]:
        raise NotImplementedError


class ManyToMany:
    """Multiple input topics, multiple output topics.

    Override ``process`` with the additional ``source`` argument (bare input
    topic name).  Return a ``dict`` keyed by bare output topic name, or
    ``None`` to produce no output for that batch (e.g. accumulation side of a
    join).
    """

    def __init__(self, config: dict) -> None:  # noqa: D107
        pass

    def process(
        self, batch: pa.RecordBatch, source: str
    ) -> dict[str, pa.RecordBatch] | None:
        raise NotImplementedError


class ZeroToOne:
    """No input topics; produces a stream of output records on a timer.

    The runner calls ``generate()`` on each tick (controlled by
    ``TICK_INTERVAL_MS`` env var, default 1000 ms).  Return ``None`` to skip
    producing for that tick.
    """

    def __init__(self, config: dict) -> None:  # noqa: D107
        pass

    def generate(self) -> pa.RecordBatch | None:
        raise NotImplementedError
