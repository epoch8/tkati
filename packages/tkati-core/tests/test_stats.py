import time

from loguru import logger
from tkati_core import LoopStats


def _stats(**kwargs) -> LoopStats:
    return LoopStats(name="test", phases=("read", "work", "write"), **kwargs)


def test_phase_timings_accumulate_across_blocks() -> None:
    stats = _stats()
    for _ in range(3):
        with stats.phase("work"):
            time.sleep(0.005)

    assert "work" in stats.phase_sec
    # Three 5ms sleeps, with generous slack for a loaded CI box.
    assert 0.015 <= stats.phase_sec["work"] < 1.0
    assert "read" not in stats.phase_sec


def test_phase_records_even_when_the_block_raises() -> None:
    """A phase that blows up still consumed wall clock, and the loop above may
    catch and continue — losing the time would silently skew the report."""
    stats = _stats()
    try:
        with stats.phase("work"):
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    assert stats.phase_sec["work"] > 0


def test_reset_clears_counters_and_restarts_the_interval() -> None:
    stats = _stats()
    stats.rows_in = 100
    stats.rows_out = 90
    stats.iterations = 5
    stats.starved_iterations = 1
    stats.record("work", 1.0)
    stats.started -= 3600

    stats.reset()

    assert stats.rows_in == 0
    assert stats.rows_out == 0
    assert stats.iterations == 0
    assert stats.starved_iterations == 0
    assert stats.phase_sec == {}
    assert time.monotonic() - stats.started < 1.0


def test_report_if_due_waits_for_the_interval() -> None:
    stats = _stats(report_interval_sec=10.0)
    stats.rows_in = 42

    stats.report_if_due()
    assert stats.rows_in == 42, "reported before the interval elapsed"

    # Rewind rather than sleep: the gate reads `started`, so this is the same
    # thing to it and keeps the test instant.
    stats.started -= 10.0
    stats.report_if_due()
    assert stats.rows_in == 0, "did not report once the interval elapsed"


def test_report_on_an_empty_interval_does_not_divide_by_zero() -> None:
    """An interval can contain no rows and no iterations — an idle node, or a
    report triggered immediately after a reset."""
    stats = _stats()
    stats.started = time.monotonic()  # elapsed ~0
    stats.report()  # must not raise ZeroDivisionError
    assert stats.iterations == 0


def test_phases_render_in_declared_order_even_when_some_never_fired() -> None:
    """Column order has to be stable across reports, so it follows `phases`
    rather than whichever phase happened to fire first this interval."""
    stats = _stats()
    with stats.phase("write"):
        pass
    with stats.phase("read"):
        pass

    lines: list[str] = []
    handler_id = logger.add(lambda m: lines.append(str(m)), level="INFO")
    try:
        stats.report()
    finally:
        logger.remove(handler_id)

    phase_line = next(line for line in lines if "read=" in line)
    assert phase_line.index("read=") < phase_line.index("work=")
    assert phase_line.index("work=") < phase_line.index("write=")
    assert "work=0.00s" in phase_line
