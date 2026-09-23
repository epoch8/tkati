import socket
import time
import urllib.request

import pytest
from prometheus_client import CollectorRegistry
from tkati_core import (
    LoopStats,
    LoopStatsCollector,
    MetricsSettings,
    start_metrics_server,
)

PHASES = ("consumer/poll", "work")


def _registry(stats: LoopStats) -> CollectorRegistry:
    registry = CollectorRegistry()
    registry.register(LoopStatsCollector(stats))
    return registry


def _phase(registry: CollectorRegistry, phase: str) -> float | None:
    return registry.get_sample_value(
        "tkati_phase_seconds_total", {"node": "test", "phase": phase}
    )


def _node(registry: CollectorRegistry, metric: str) -> float | None:
    return registry.get_sample_value(metric, {"node": "test"})


def test_every_declared_phase_is_exported_before_it_fires() -> None:
    """Each series exists at 0 from the first scrape, so a rate over it starts
    from a known value instead of from whenever the phase first ran."""
    registry = _registry(LoopStats(name="test", phases=PHASES))

    assert _phase(registry, "consumer/poll") == pytest.approx(0.0)
    assert _phase(registry, "work") == pytest.approx(0.0)


def test_counters_carry_the_loop_stats() -> None:
    stats = LoopStats(name="test", phases=PHASES)
    registry = _registry(stats)
    stats.record("work", 1.5)
    stats.rows_in = 10
    stats.rows_out = 8
    stats.iterations = 3
    stats.starved_iterations = 1

    assert _phase(registry, "work") == pytest.approx(1.5)
    assert _node(registry, "tkati_rows_in_total") == 10
    assert _node(registry, "tkati_rows_out_total") == 8
    assert _node(registry, "tkati_iterations_total") == 3
    assert _node(registry, "tkati_starved_iterations_total") == 1


def test_counters_do_not_go_backwards_across_a_report() -> None:
    """report() resets the interval. A counter that dropped with it would read
    as a process restart to Prometheus and wreck every rate() over it."""
    stats = LoopStats(name="test", phases=PHASES)
    registry = _registry(stats)
    stats.record("work", 1.0)
    stats.rows_in = 10
    before = (_phase(registry, "work"), _node(registry, "tkati_rows_in_total"))

    stats.report()

    after = (_phase(registry, "work"), _node(registry, "tkati_rows_in_total"))
    assert after == before


def test_wall_clock_bounds_the_sum_of_phases() -> None:
    """Wall seconds are the 100% denominator: phases are timed inside it, so
    their sum can only fall short of it — the unaccounted remainder."""
    stats = LoopStats(name="test", phases=PHASES)
    registry = _registry(stats)
    for _ in range(3):
        with stats.phase("work"):
            time.sleep(0.005)
        with stats.phase("consumer/poll"):
            time.sleep(0.005)

    phases = (_phase(registry, "work") or 0) + (_phase(registry, "consumer/poll") or 0)
    wall = _node(registry, "tkati_wall_seconds_total")
    assert wall is not None and wall >= phases > 0


def test_disabled_settings_start_nothing() -> None:
    registry = CollectorRegistry()
    server = start_metrics_server(
        MetricsSettings(enabled=False), LoopStats(name="test", phases=()), registry
    )

    assert server is None
    assert list(registry.collect()) == []


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_server_serves_the_stats_over_http() -> None:
    stats = LoopStats(name="test", phases=PHASES)
    stats.rows_in = 42
    port = _free_port()

    server = start_metrics_server(
        MetricsSettings(port=port, addr="127.0.0.1"), stats, CollectorRegistry()
    )
    assert server is not None
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics") as resp:
            body = resp.read().decode()
    finally:
        server.shutdown()
        server.server_close()

    assert 'tkati_rows_in_total{node="test"} 42.0' in body
    assert 'tkati_phase_seconds_total{node="test",phase="consumer/poll"} 0.0' in body


def test_port_must_be_a_real_tcp_port() -> None:
    with pytest.raises(ValueError):
        MetricsSettings(port=0)
