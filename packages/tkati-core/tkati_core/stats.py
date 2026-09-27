"""Wall-clock accounting for a node's main loop.

Answers one question: of the time a node spends, how much went to reading and
parsing, how much to each processing step, and how much to producing? Nodes
time their phases against a `LoopStats` and let it log a breakdown on an
interval. Work on another thread, such as a node's read-ahead, is timed
against a `PhaseStats` of its own, so each line's shares are one thread's.

Deliberately not tied to any one node — the phase names and the reporting
cadence are constructor arguments, because different nodes have different
pipelines.
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
class PhaseStats:
    """Wall-clock spent in each phase of one thread's work, accumulated
    between reports, and logged as one `perf <label>: ...` line.

    `phases` is an explicit ordered tuple rather than being derived from
    whatever `phase_sec` happens to contain: a phase that doesn't fire during
    an interval's first iteration would otherwise shift the column order from
    one report to the next, and a stable order is what makes two consecutive
    lines comparable at a glance.

    One instance per thread: the line's percentages are shares of that
    thread's time, which two threads' phases on one line would overlap.
    Consumers and producers take a `PhaseStats` to time themselves against.
    """

    phases: tuple[str, ...]
    # Names the line: `perf <label>: ...`, or `perf: ...` when empty.
    label: str = ""
    phase_sec: dict[str, float] = field(default_factory=dict)
    started: float = field(default_factory=time.monotonic)

    # Totals from intervals already reported, folded in by `reset()`. The
    # fields above restart every interval; these never do, so totals can back
    # monotonic counters.
    _total_phase_sec: dict[str, float] = field(
        default_factory=dict, init=False, repr=False
    )
    # Taken by `record()`, `reset()` and `LoopStats.totals()`. The totals are
    # read from a metrics server thread: without the lock a scrape could land
    # between the fold and the zeroing in `reset()` and see an interval
    # counted twice or not at all, and a counter that jumps backwards reads as
    # a restart to Prometheus. `record()` takes it because an instance can be
    # timed on one thread and reset on another, as a node's read-ahead stats
    # are: the get-then-set could otherwise straddle the `reset()` and write a
    # stale sum into the new interval. It runs a few times per batch, so the
    # lock costs nothing that shows.
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )

    def record(self, phase: str, seconds: float) -> None:
        with self._lock:
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
        running totals."""
        with self._lock:
            self._fold()

    def _fold(self) -> None:
        """Move the interval into the totals and restart it. The caller holds
        the lock."""
        for name, sec in self.phase_sec.items():
            self._total_phase_sec[name] = self._total_phase_sec.get(name, 0.0) + sec
        self.phase_sec = {}
        self.started = time.monotonic()

    def report(self) -> None:
        """Log the interval's phase line, then reset.

        Percentages are of the wall-clock interval, not of each other, so they
        deliberately do not sum to 100 — the shortfall is time in none of the
        named phases, which keeps unaccounted work visible.
        """
        self._log_phases(self._elapsed())
        self.reset()

    def _elapsed(self) -> float:
        return max(time.monotonic() - self.started, 1e-9)

    def _log_phases(self, elapsed: float) -> None:
        phases = " ".join(
            f"{name}={(sec := self.phase_sec.get(name, 0.0)):.2f}s ({sec / elapsed * 100:.0f}%)"
            for name in self.phases
        )
        prefix = f"perf {self.label}:" if self.label else "perf:"
        logger.info(f"{prefix} {phases}")


@dataclass
class LoopStats(PhaseStats):
    """A node loop's `PhaseStats`, plus what the loop counts: iterations,
    starved iterations, and rows in and out. Its report leads with a
    `perf over ...` line for those, and it backs the Prometheus metrics."""

    report_interval_sec: float = 10.0

    iterations: int = 0
    starved_iterations: int = 0
    rows_in: int = 0
    rows_out: int = 0

    _created: float = field(default_factory=time.monotonic, init=False, repr=False)
    _total_rows_in: int = field(default=0, init=False, repr=False)
    _total_rows_out: int = field(default=0, init=False, repr=False)
    _total_iterations: int = field(default=0, init=False, repr=False)
    # The `+=` on the counters above don't take the lock: only the loop thread
    # updates them, and each is a single int update the GIL makes atomic, so a
    # scrape sees either the old value or the new one.
    _total_starved: int = field(default=0, init=False, repr=False)

    def _fold(self) -> None:
        super()._fold()
        self._total_rows_in += self.rows_in
        self._total_rows_out += self.rows_out
        self._total_iterations += self.iterations
        self._total_starved += self.starved_iterations
        self.iterations = 0
        self.starved_iterations = 0
        self.rows_in = 0
        self.rows_out = 0

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

        Percentages are as for `PhaseStats.report`.

        Rows in and out are counted per committed batch, both at the commit
        (see `_NodeBase._commit`), so "dropped" is exact for the batches
        committed in the interval, however far commits lag reads.

        Beware the starved case: a node's read phase typically blocks until
        the batch fills or the batch timeout expires, so on an under-fed node
        it approaches 100% and none of the other figures mean anything. That
        is what `input-starved` counts.
        """
        elapsed = self._elapsed()
        logger.info(
            f"perf over {elapsed:.3g}s: {self.rows_in} rows in, "
            f"{self.rows_out} out ({self.rows_in - self.rows_out} dropped), "
            f"{self.iterations} iterations ({self.starved_iterations} input-starved)"
        )
        self._log_phases(elapsed)
        self.reset()

    def report_if_due(self) -> bool:
        """Report only once `report_interval_sec` has elapsed, and say whether
        it did. Cheap enough to call every iteration, which is the intended
        usage."""
        if time.monotonic() - self.started >= self.report_interval_sec:
            self.report()
            return True
        return False
