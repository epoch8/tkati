"""Prometheus export of a node's `LoopStats`.

The same numbers as the periodic perf log line, as monotonic counters: shares
and rates are left to PromQL rather than computed here, which is the
Prometheus convention and is what survives scrape gaps and restarts. The
"% of the interval" figures on the log line are

    rate(tkati_phase_seconds_total[1m])
      / ignoring(phase) group_left rate(tkati_wall_seconds_total[1m])

Wall clock is its own metric rather than a `phase="total"` series: with a
total inside the phase metric, `sum without (phase)` over phases would count
it twice.
"""

from collections.abc import Iterator
from wsgiref.simple_server import WSGIServer

from loguru import logger
from prometheus_client import REGISTRY, CollectorRegistry, start_http_server
from prometheus_client.core import CounterMetricFamily, Metric
from pydantic import BaseModel, field_validator

from tkati_core.stats import LoopStats


class LoopStatsCollector:
    """Exposes one `LoopStats` to a Prometheus registry.

    A custom collector rather than `prometheus_client.Counter`s updated as the
    loop runs: values are read from `LoopStats.totals()` at scrape time, so the
    loop pays nothing for being exported and `LoopStats` needs no hooks.
    """

    def __init__(self, stats: LoopStats) -> None:
        self._stats = stats

    def collect(self) -> Iterator[Metric]:
        totals = self._stats.totals()

        phases = CounterMetricFamily(
            "tkati_phase_seconds",
            "Wall-clock seconds spent in each phase of the node loop.",
            labels=["phase"],
        )
        # Every declared phase, in declared order, even before it has fired —
        # so each series exists from the first scrape with a known starting
        # value. Phases timed but not declared are still exported, after them.
        names = list(self._stats.phases)
        names += [name for name in totals.phase_sec if name not in names]
        for name in names:
            phases.add_metric([name], totals.phase_sec.get(name, 0.0))
        yield phases

        for metric, documentation, value in (
            (
                "tkati_wall_seconds",
                (
                    "Wall-clock seconds since the node loop's stats were created. "
                    "The denominator for phase shares."
                ),
                totals.wall_sec,
            ),
            ("tkati_rows_in", "Rows read from the input.", totals.rows_in),
            ("tkati_rows_out", "Rows written to the output.", totals.rows_out),
            ("tkati_iterations", "Node loop iterations.", totals.iterations),
            (
                "tkati_starved_iterations",
                (
                    "Iterations that drained the input and waited out the batch "
                    "timeout, so were not CPU-bound."
                ),
                totals.starved_iterations,
            ),
        ):
            yield CounterMetricFamily(metric, documentation, value=value)


class MetricsSettings(BaseModel):
    """Where to serve `/metrics`. On by default; set `enabled = false` in the
    node's `[metrics]` section, or `METRICS__ENABLED=false`, to turn it off."""

    enabled: bool = True
    port: int = 8000
    addr: str = "0.0.0.0"

    @field_validator("port")
    @classmethod
    def _port(cls, v: int) -> int:
        # 0 would bind a random port nobody could be told to scrape.
        if not 1 <= v <= 65535:
            raise ValueError("must be a TCP port in [1, 65535]")
        return v


def start_metrics_server(
    settings: MetricsSettings,
    stats: LoopStats,
    registry: CollectorRegistry = REGISTRY,
) -> WSGIServer | None:
    """Serve `stats` on `settings.addr:settings.port/metrics` from a daemon
    thread. Returns the server (so tests can shut it down), or None when
    metrics are disabled."""
    if not settings.enabled:
        logger.info("Prometheus metrics disabled")
        return None

    registry.register(LoopStatsCollector(stats))
    server, _thread = start_http_server(settings.port, settings.addr, registry)
    logger.info(
        f"Serving Prometheus metrics on http://{settings.addr}:{settings.port}/metrics"
    )
    return server
