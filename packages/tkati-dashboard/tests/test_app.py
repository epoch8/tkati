from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from tkati_dashboard import lag, snapshot
from tkati_dashboard.app import create_app


def test_index_serves_html(sample_dataflow_dir: Path) -> None:
    client = TestClient(create_app(sample_dataflow_dir))

    response = client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_graph_endpoint_returns_nodes_and_edges(sample_dataflow_dir: Path) -> None:
    client = TestClient(create_app(sample_dataflow_dir))

    response = client.get("/api/graph")

    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "sample-dataflow"

    nodes_by_id = {node["id"]: node for node in body["nodes"]}
    assert nodes_by_id["raw-orders"]["group"] == "source-sink"
    assert nodes_by_id["raw-orders"]["connection"] == {
        "broker": "redpanda:29092",
        "topic": "raw_orders",
    }
    assert nodes_by_id["raw-orders"]["schema"] == {
        "id": "string",
        "time": "timestamp[ms]",
        "amount": "int64",
    }
    assert nodes_by_id["dedup"]["group"] == "processing-node"
    assert nodes_by_id["dedup"]["config"] == {"field": "id", "window_hours": 3}

    edges_by_endpoints = {(e["from"], e["to"]): e for e in body["edges"]}
    assert edges_by_endpoints[("raw-orders", "dedup")]["kind"] == "stream"
    assert edges_by_endpoints[("raw-orders", "dedup")]["consumer"] == {
        "group_id": "orders-dedup"
    }
    assert edges_by_endpoints[("dedup", "deduped-orders")]["consumer"] is None


def test_graph_endpoint_returns_422_on_invalid_dataflow(
    invalid_dataflow_dir: Path,
) -> None:
    client = TestClient(create_app(invalid_dataflow_dir))

    response = client.get("/api/graph")

    assert response.status_code == 422
    assert "detail" in response.json()


def test_snapshot_endpoint_returns_events(
    sample_dataflow_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        snapshot, "fetch_kafka_snapshot", lambda broker, topic, **kw: [{"id": "1"}]
    )
    client = TestClient(create_app(sample_dataflow_dir))

    response = client.get("/api/nodes/raw-orders/snapshot")

    assert response.status_code == 200
    assert response.json() == {"events": [{"id": "1"}]}


def test_snapshot_endpoint_404_for_unknown_node(sample_dataflow_dir: Path) -> None:
    client = TestClient(create_app(sample_dataflow_dir))

    response = client.get("/api/nodes/does-not-exist/snapshot")

    assert response.status_code == 404


def test_snapshot_endpoint_404_for_non_kafka_node(sample_dataflow_dir: Path) -> None:
    client = TestClient(create_app(sample_dataflow_dir))

    response = client.get("/api/nodes/dedup/snapshot")

    assert response.status_code == 404


def test_snapshot_endpoint_502_on_fetch_error(
    sample_dataflow_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise(broker: str, topic: str, **kw: object) -> list:
        raise snapshot.SnapshotError("broker unreachable")

    monkeypatch.setattr(snapshot, "fetch_kafka_snapshot", _raise)
    client = TestClient(create_app(sample_dataflow_dir))

    response = client.get("/api/nodes/raw-orders/snapshot")

    assert response.status_code == 502
    assert response.json()["detail"] == "broker unreachable"


def test_consumer_lag_endpoint_returns_lag(
    sample_dataflow_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = []

    def _fetch(broker: str, topic: str, group_id: str, **kw: object) -> dict:
        calls.append((broker, topic, group_id))
        return {"total_lag": 4, "partitions": [{"partition": 0, "lag": 4}]}

    monkeypatch.setattr(lag, "fetch_consumer_lag", _fetch)
    client = TestClient(create_app(sample_dataflow_dir))

    response = client.get(
        "/api/nodes/raw-orders/consumer-lag", params={"group_id": "orders-dedup"}
    )

    assert response.status_code == 200
    assert response.json()["total_lag"] == 4
    assert calls == [("redpanda:29092", "raw_orders", "orders-dedup")]


def test_consumer_lag_endpoint_404_for_non_kafka_node(
    sample_dataflow_dir: Path,
) -> None:
    client = TestClient(create_app(sample_dataflow_dir))

    response = client.get(
        "/api/nodes/dedup/consumer-lag", params={"group_id": "orders-dedup"}
    )

    assert response.status_code == 404


def test_consumer_lag_endpoint_502_on_fetch_error(
    sample_dataflow_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise(broker: str, topic: str, group_id: str, **kw: object) -> dict:
        raise lag.LagError("group not found")

    monkeypatch.setattr(lag, "fetch_consumer_lag", _raise)
    client = TestClient(create_app(sample_dataflow_dir))

    response = client.get(
        "/api/nodes/raw-orders/consumer-lag", params={"group_id": "orders-dedup"}
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "group not found"
