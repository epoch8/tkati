"""Wall-clock accounting for a node's main loop.

Answers one question: of the time a node spends, how much went to reading and
parsing, how much to each processing step, and how much to producing? Nodes
time their phases against a `LoopStats` and let it log a breakdown on an
interval.

Deliberately not tied to any one node — the phase names, the log prefix and
the reporting cadence are all constructor arguments, because different nodes
have different pipelines.
"""

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from loguru import logger


@dataclass
class LoopStats:
    """Wall-clock spent in each phase of a node loop, accumulated between
    reports.

    `phases` is an explicit ordered tuple rather than being derived from
    whatever `phase_sec` happens to contain: a phase that doesn't fire during
    an interval's first iteration would otherwise shift the column order from
    one report to the next, and a stable order is what makes two consecutive
    lines comparable at a glance.
    """

    name: str
    phases: tuple[str, ...]
    report_interval_sec: float = 10.0

    iterations: int = 0
    starved_iterations: int = 0
    rows_in: int = 0
    rows_out: int = 0
    phase_sec: dict[str, float] = field(default_factory=dict)
    started: float = field(default_factory=time.monotonic)

    def record(self, phase: str, seconds: float) -> None:
        self.phase_sec[phase] = self.phase_sec.get(phase, 0.0) + seconds

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        """Time a block and add it to `name`'s running total."""
        started = time.perf_counter()
        try:
            yield
        finally:
            self.record(name, time.perf_counter() - started)

    def reset(self) -> None:
        self.iterations = 0
        self.starved_iterations = 0
        self.rows_in = 0
        self.rows_out = 0
        self.phase_sec = {}
        self.started = time.monotonic()

    def report(self) -> None:
        """Log one interval's breakdown, then reset.

        Percentages are of the wall-clock interval, not of each other, so they
        deliberately do not sum to 100 — the shortfall is time in none of the
        named phases, which keeps unaccounted work visible.

        Beware the starved case: a node's read phase typically blocks until
        the batch fills or the batch timeout expires, so on an under-fed node
        it approaches 100% and none of the other figures mean anything. That
        is what `input-starved` counts.
        """
        elapsed = max(time.monotonic() - self.started, 1e-9)

        logger.info(
            f"{self.name} perf over {elapsed:.3g}s: {self.rows_in} rows in, "
            f"{self.rows_out} out ({self.rows_in - self.rows_out} dropped), "
            f"{self.iterations} iterations ({self.starved_iterations} input-starved)"
        )
        phases = " ".join(
            f"{name}={(sec := self.phase_sec.get(name, 0.0)):.2f}s ({sec / elapsed * 100:.0f}%)"
            for name in self.phases
        )
        logger.info(f"{self.name} perf: {phases}")
        self.reset()

    def report_if_due(self) -> None:
        """Report only once `report_interval_sec` has elapsed. Cheap enough to
        call every iteration, which is the intended usage."""
        if time.monotonic() - self.started >= self.report_interval_sec:
            self.report()
