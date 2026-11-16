from contextlib import closing
from decimal import Decimal

import pyarrow as pa
import pytest
from prometheus_client import CollectorRegistry, Counter, generate_latest
from tkati_core import DeliveryError
from tkati_core.testing import memory_pipelined_node
from tkati_node_dedup import main as dedup_main
from tkati_node_dedup.main import _PHASES, run
from tkati_node_dedup.settings import AppSettings, DedupSettings
from tkati_node_dedup.store import BucketedDedupStore


@pytest.fixture
def registry(monkeypatch) -> CollectorRegistry:
    registry = CollectorRegistry()
    counter = Counter(
        "tkati_node_dedup_dropped_field",
        "Dropped field sums",
        ["field"],
        registry=registry,
    )
    monkeypatch.setattr("tkati_node_dedup.main._DROPPED_FIELDS", counter)
    return registry


def _total(registry: CollectorRegistry, field: str) -> float:
    value = registry.get_sample_value(
        "tkati_node_dedup_dropped_field_total", {"field": field}
    )
    assert value is not None
    return value


def test_sums_only_dropped_rows_across_batches(tmp_path, registry) -> None:
    # Null dedup keys survive even when the whole batch is read again.
    table = pa.table(
        {
            "uid": ["a", "a", None, "b", "b"],
            "traffic_in": [100, 20, 900, 200, 30],
            "traffic_out": [1.0, 2.5, 90.0, 3.0, 4.5],
        }
    )
    with closing(BucketedDedupStore(str(tmp_path), window_hours=3)) as store:
        node, _, producer = memory_pipelined_node([table], phases=_PHASES)
        with node:
            # Repeating a configured field must not count it twice.
            run(
                node,
                store,
                "uid",
                ["traffic_in", "traffic_out", "traffic_in", "missing"],
            )
        assert _total(registry, "traffic_in") == 50
        assert _total(registry, "traffic_out") == 7
        assert _total(registry, "missing") == 0
        assert producer is not None
        sent = producer.sent[0]
        assert isinstance(sent, pa.Table)
        assert sent.column("uid").to_pylist() == ["a", None, "b"]

        node, _, _ = memory_pipelined_node([table], phases=_PHASES)
        with node:
            run(node, store, "uid", ["traffic_in", "traffic_out"])
        assert _total(registry, "traffic_in") == 400
        assert _total(registry, "traffic_out") == 18


@pytest.mark.parametrize(
    "values, expected",
    [
        (pa.array([999, 7, None, -3, 0], type=pa.int64()), 7),
        # The sum would overflow uint64 if aggregated in its original type.
        (pa.array([0, 2**63, 2**63], type=pa.uint64()), float(2**64)),
        (
            pa.array(
                [999, 2.5, None, -1, float("nan"), float("inf"), -float("inf"), 3.5]
            ),
            6,
        ),
        (
            pa.array(
                [Decimal("999"), Decimal("2.5"), None, Decimal("-1")],
                type=pa.decimal128(10, 1),
            ),
            2.5,
        ),
        (pa.array([0.0, 1e308, 1e308]), 0),
        (pa.array(["999", "7", None]), 0),
        (pa.array([True, True, False]), 0),
        (pa.array([None, None]), 0),
        (pa.array([[999], [7]]), 0),
        (pa.array([{"value": 999}, {"value": 7}]), 0),
        (pa.array([0, 7], type=pa.timestamp("ms")), 0),
        (pa.array([b"999", b"7"]), 0),
    ],
)
def test_numeric_and_invalid_values(tmp_path, registry, values, expected) -> None:
    table = pa.table({"uid": ["a"] * len(values), "traffic_in": values})
    node, consumer, _ = memory_pipelined_node([table], phases=_PHASES)
    with closing(BucketedDedupStore(str(tmp_path), window_hours=3)) as store, node:
        run(node, store, "uid", ["traffic_in"])
    assert consumer.commits == [0]
    assert _total(registry, "traffic_in") == expected


def test_pending_duplicates_counted_only_after_commit(tmp_path, registry) -> None:
    tables = [
        pa.table({"uid": ["a"], "traffic_in": [100]}),
        pa.table({"uid": ["a", "b", "b"], "traffic_in": [20, 200, 30]}),
    ]
    node, consumer, producer = memory_pipelined_node(
        tables, phases=_PHASES, deliver="manual"
    )
    assert producer is not None

    def release(tag: int) -> None:
        assert _total(registry, "traffic_in") == 0
        producer.release(tag)

    producer.on_wait = release
    with closing(BucketedDedupStore(str(tmp_path), window_hours=3)) as store, node:
        run(node, store, "uid", ["traffic_in"])
    assert consumer.commits == [0, 1]
    assert _total(registry, "traffic_in") == 50


def test_failed_batch_not_counted_and_retry_counted_once(tmp_path, registry) -> None:
    table = pa.table({"uid": ["a", "a"], "traffic_in": [100, 20]})
    with closing(BucketedDedupStore(str(tmp_path), window_hours=3)) as store:
        node, consumer, producer = memory_pipelined_node([table], phases=_PHASES)
        assert producer is not None
        producer.fail(1, DeliveryError("gave up"))
        with pytest.raises(DeliveryError), node:
            run(node, store, "uid", ["traffic_in"])
        assert consumer.commits == []
        assert _total(registry, "traffic_in") == 0

        node, _, _ = memory_pipelined_node([table], phases=_PHASES)
        with node:
            run(node, store, "uid", ["traffic_in"])
        assert _total(registry, "traffic_in") == 20


@pytest.mark.parametrize(
    "tables",
    [
        [None],
        [pa.table({"traffic_in": [100, 20]})],
        [pa.table({"uid": ["a", "b"], "traffic_in": [100, 20]})],
        [pa.table({"uid": ["a", "a"]})],
    ],
)
def test_zero_is_exposed_without_values_to_count(tmp_path, registry, tables) -> None:
    node, _, _ = memory_pipelined_node(tables, phases=_PHASES)
    with closing(BucketedDedupStore(str(tmp_path), window_hours=3)) as store, node:
        run(node, store, "uid", ["traffic_in"])
    assert _total(registry, "traffic_in") == 0
    assert (
        b'tkati_node_dedup_dropped_field_total{field="traffic_in"} 0.0'
        in generate_latest(registry)
    )


def test_no_extra_series_by_default(tmp_path, registry) -> None:
    table = pa.table({"uid": ["a", "a"], "traffic_in": [100, 20]})
    node, _, _ = memory_pipelined_node([table], phases=_PHASES)
    with closing(BucketedDedupStore(str(tmp_path), window_hours=3)) as store, node:
        run(node, store, "uid")
    assert (
        registry.get_sample_value(
            "tkati_node_dedup_dropped_field_total", {"field": "traffic_in"}
        )
        is None
    )
    assert DedupSettings(field="uid").counter_fields == []


def test_environment_counter_fields(monkeypatch, test_settings) -> None:
    monkeypatch.setenv("DEDUP__COUNTER_FIELDS", '["traffic_in", "traffic_out"]')
    settings = AppSettings(
        input=test_settings.input, output=test_settings.output, dedup={"field": "uid"}
    )
    assert settings.dedup.counter_fields == ["traffic_in", "traffic_out"]


def test_main_uses_configured_counter_fields(
    monkeypatch, test_settings, registry
) -> None:
    test_settings.dedup.counter_fields = ["traffic_in"]
    table = pa.table(
        {
            "uid": ["a", "a", "a"],
            "traffic_in": pa.chunked_array([[100, 20], [30]]),
        }
    )
    node, _, _ = memory_pipelined_node([table], phases=_PHASES)
    monkeypatch.setattr(dedup_main, "AppSettings", lambda: test_settings)
    monkeypatch.setattr(
        dedup_main.PipelinedNode, "from_settings", lambda *args, **kwargs: node
    )
    dedup_main.main()
    assert _total(registry, "traffic_in") == 50
