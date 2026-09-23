"""Wall-clock accounting for a node's main loop.

Answers one question: of the time a node spends, how much went to reading and
parsing, how much to each processing step, and how much to producing? Nodes
time their phases against a `LoopStats` and let it log a breakdown on an
interval.

Deliberately not tied to any one node — the phase names, the log prefix and
the reporting cadence are all constructor arguments, because different nodes
have different pipelines.
"""

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from loguru import logger


@dataclass(frozen=True)
class LoopStatsTotals:
    """Everything a `LoopStats` has counted since it was created, across all
    reports. Monotonic, which is what a Prometheus counter requires."""

    phase_sec: dict[str, float]
    rows_in: int
    rows_out: int
    iterations: int
    starved_iterations: int
    # Wall clock since the LoopStats was created: the denominator that turns
    # phase seconds into a share of time, as the log line's percentages do.
    wall_sec: float


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

    # Totals from intervals already reported, folded in by `reset()`. The
    # fields above restart every interval; these never do, so `totals()` can
    # back monotonic counters.
    _created: float = field(default_factory=time.monotonic, init=False, repr=False)
    _total_phase_sec: dict[str, float] = field(
        default_factory=dict, init=False, repr=False
    )
    _total_rows_in: int = field(default=0, init=False, repr=False)
    _total_rows_out: int = field(default=0, init=False, repr=False)
    _total_iterations: int = field(default=0, init=False, repr=False)
    _total_starved: int = field(default=0, init=False, repr=False)
    # Taken by `reset()` and `totals()` only — `totals()` is called from a
    # metrics server thread. Without it a scrape could land between the fold
    # and the zeroing in `reset()` and see an interval counted twice or not at
    # all, and a counter that jumps backwards reads as a restart to
    # Prometheus. `record()` and the `+=` on the counters above deliberately
    # don't take it: each is a single dict or int update the GIL already makes
    # atomic, so a scrape sees either the old value or the new one, and the hot
    # path stays lock-free.
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )

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
        """Start a new interval. What the old one counted is kept in the
        running totals behind `totals()`."""
        with self._lock:
            for name, sec in self.phase_sec.items():
                self._total_phase_sec[name] = self._total_phase_sec.get(name, 0.0) + sec
            self._total_rows_in += self.rows_in
            self._total_rows_out += self.rows_out
            self._total_iterations += self.iterations
            self._total_starved += self.starved_iterations

            self.iterations = 0
            self.starved_iterations = 0
            self.rows_in = 0
            self.rows_out = 0
            self.phase_sec = {}
            self.started = time.monotonic()

    def totals(self) -> LoopStatsTotals:
        """Everything counted since creation, including the interval that
        hasn't been reported yet. Safe to call from another thread."""
        with self._lock:
            phase_sec = dict(self._total_phase_sec)
            for name, sec in dict(self.phase_sec).items():
                phase_sec[name] = phase_sec.get(name, 0.0) + sec
            return LoopStatsTotals(
                phase_sec=phase_sec,
                rows_in=self._total_rows_in + self.rows_in,
                rows_out=self._total_rows_out + self.rows_out,
                iterations=self._total_iterations + self.iterations,
                starved_iterations=self._total_starved + self.starved_iterations,
                wall_sec=time.monotonic() - self._created,
            )

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
