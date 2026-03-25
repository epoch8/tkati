"""Example pipeline nodes for the clicks pipeline.

Topology:
    ClickGenerator  →  clicks_raw  →  ClickEnricher  →  clicks_enriched  →  ClickAggregator  →  clicks_hourly
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc

from tkati_node import OneToOne, ZeroToOne


# ---------------------------------------------------------------------------
# Stub helpers — replace with real implementations as needed
# ---------------------------------------------------------------------------

def _lookup_country(urls: pa.Array, geoip_db: str) -> pa.Array:
    """Stub: return a constant country string for every URL."""
    return pa.array(["US"] * len(urls), type=pa.utf8())


def _detect_device(urls: pa.Array) -> pa.Array:
    """Stub: classify device type from the URL (User-Agent not available here).

    In a real implementation you would pass the User-Agent column.
    """
    return pa.array(["desktop"] * len(urls), type=pa.utf8())


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

class ClickGenerator(ZeroToOne):
    """Generate synthetic raw_click events for testing and development.

    Produces a batch of random clicks on every tick.  The batch size is
    controlled by the ``batch_size`` config key (default 100).
    """

    _URLS = [
        "https://example.com/home",
        "https://example.com/products",
        "https://example.com/about",
        "https://example.com/checkout",
        "https://example.com/blog",
    ]

    def __init__(self, config: dict) -> None:
        self.batch_size: int = int(config.get("batch_size", 100))

    def generate(self) -> pa.RecordBatch:
        now = datetime.now(timezone.utc)
        n = self.batch_size
        return pa.record_batch({
            "event_id": pa.array([str(uuid.uuid4()) for _ in range(n)], type=pa.utf8()),
            "user_id":  pa.array([random.randint(1, 10_000) for _ in range(n)], type=pa.int64()),
            "url":      pa.array([random.choice(self._URLS) for _ in range(n)], type=pa.utf8()),
            "ts":       pa.array([now] * n, type=pa.timestamp("ms", tz="UTC")),
        })


class ClickEnricher(OneToOne):
    """Enrich raw clicks with country (via GeoIP) and device type.

    Input schema:  raw_click   — event_id, user_id, url, ts
    Output schema: enriched_click — raw_click + country, device
    """

    def __init__(self, config: dict) -> None:
        self.geoip_db: str = config.get("geoip_db", "")

    def process(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        urls = batch.column("url")
        country = _lookup_country(urls, self.geoip_db)
        device = _detect_device(urls)
        return batch.append_column("country", country).append_column("device", device)


class ClickAggregator(OneToOne):
    """Aggregate enriched clicks into hourly counts per country.

    Input schema:  enriched_click — event_id, user_id, url, ts, country, device
    Output schema: clicks_hourly  — hour, country, count
    """

    def __init__(self, config: dict) -> None:
        pass

    def process(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        # Truncate timestamp to the start of the hour via integer arithmetic
        ts = batch.column("ts")
        ms_per_hour = 3_600_000
        ts_int = pc.cast(ts, pa.int64())
        hour = pc.cast(
            pc.multiply(pc.divide(ts_int, ms_per_hour), ms_per_hour),   # pyright: ignore[reportAttributeAccessIssue]
            pa.timestamp("ms", tz="UTC"),
        )

        country = batch.column("country")

        # Group by (hour, country) and count
        table = pa.table({"hour": hour, "country": country})
        grouped = (
            table.group_by(["hour", "country"])
            .aggregate([("hour", "count")])
            .rename_columns(["hour", "country", "count"])
        )

        return grouped.to_batches()[0] if len(grouped) else pa.record_batch(
            {
                "hour": pa.array([], type=pa.timestamp("ms", tz="UTC")),
                "country": pa.array([], type=pa.utf8()),
                "count": pa.array([], type=pa.int64()),
            }
        )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def _raw_batch(*rows: tuple) -> pa.RecordBatch:
    """Build a raw_click RecordBatch from (event_id, user_id, url, ts) tuples."""
    event_ids, user_ids, urls, tss = zip(*rows)
    return pa.record_batch({
        "event_id": pa.array(event_ids, type=pa.utf8()),
        "user_id":  pa.array(user_ids,  type=pa.int64()),
        "url":      pa.array(urls,       type=pa.utf8()),
        "ts":       pa.array(tss,        type=pa.timestamp("ms", tz="UTC")),
    })


def _enriched_batch(*rows: tuple) -> pa.RecordBatch:
    """Build an enriched_click RecordBatch from (event_id, user_id, url, ts, country, device) tuples."""
    event_ids, user_ids, urls, tss, countries, devices = zip(*rows)
    return pa.record_batch({
        "event_id": pa.array(event_ids, type=pa.utf8()),
        "user_id":  pa.array(user_ids,  type=pa.int64()),
        "url":      pa.array(urls,       type=pa.utf8()),
        "ts":       pa.array(tss,        type=pa.timestamp("ms", tz="UTC")),
        "country":  pa.array(countries,  type=pa.utf8()),
        "device":   pa.array(devices,    type=pa.utf8()),
    })


H10 = datetime(2024, 1, 1, 10,  0, tzinfo=timezone.utc)
H10_MID = datetime(2024, 1, 1, 10, 30, tzinfo=timezone.utc)
H11 = datetime(2024, 1, 1, 11,  0, tzinfo=timezone.utc)


def test_enricher_appends_country_and_device():
    batch = _raw_batch(
        ("e1", 1, "http://a.com", H10),
        ("e2", 2, "http://b.com", H10_MID),
    )
    result = ClickEnricher(config={}).process(batch)

    assert result.schema.get_field_index("country") >= 0
    assert result.schema.get_field_index("device") >= 0
    assert result.column("country").to_pylist() == ["US", "US"]
    assert result.column("device").to_pylist() == ["desktop", "desktop"]
    assert len(result) == 2


def test_enricher_preserves_input_columns():
    batch = _raw_batch(("e1", 42, "http://x.com", H10))
    result = ClickEnricher(config={}).process(batch)

    assert result.column("event_id")[0].as_py() == "e1"
    assert result.column("user_id")[0].as_py() == 42


def test_aggregator_counts_by_hour_and_country():
    batch = _enriched_batch(
        ("e1", 1, "http://a.com", H10,     "US", "desktop"),
        ("e2", 2, "http://b.com", H10_MID, "US", "desktop"),
        ("e3", 3, "http://c.com", H11,     "US", "mobile"),
    )
    result = ClickAggregator(config={}).process(batch)
    sorted_table = pa.table({
        "hour":    result.column("hour"),
        "country": result.column("country"),
        "count":   result.column("count"),
    }).sort_by("hour")

    assert len(sorted_table) == 2
    assert sorted_table.column("count").to_pylist() == [2, 1]


def test_aggregator_empty_batch():
    batch = pa.record_batch({
        "event_id": pa.array([], type=pa.utf8()),
        "user_id":  pa.array([], type=pa.int64()),
        "url":      pa.array([], type=pa.utf8()),
        "ts":       pa.array([], type=pa.timestamp("ms", tz="UTC")),
        "country":  pa.array([], type=pa.utf8()),
        "device":   pa.array([], type=pa.utf8()),
    })
    result = ClickAggregator(config={}).process(batch)
    assert len(result) == 0
