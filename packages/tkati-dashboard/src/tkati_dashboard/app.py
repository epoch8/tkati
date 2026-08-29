from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from tkati_dashboard import lag, snapshot
from tkati_dashboard.dataflow import (
    SOURCE_SINK_TYPES,
    DataflowValidationError,
    NodeDef,
    load_dataflow,
)

STATIC_DIR = Path(__file__).parent / "static"


def _graph_json(directory: Path) -> dict[str, Any]:
    dataflow = load_dataflow(directory)

    nodes = [
        {
            "id": node_id,
            "label": node.name or node_id,
            "type": node.type,
            "group": "source-sink"
            if node.type in SOURCE_SINK_TYPES
            else "processing-node",
            "schema": node.schema,
            "connection": node.connection,
            "config": node.config,
        }
        for node_id, node in dataflow.nodes.items()
    ]
    edges = [
        {
            "from": edge.from_,
            "to": edge.to,
            "kind": edge.kind,
            "consumer": edge.consumer,
        }
        for edge in dataflow.edges
    ]
    return {"name": dataflow.name, "nodes": nodes, "edges": edges}


def _get_node(directory: Path, node_id: str) -> NodeDef:
    dataflow = load_dataflow(directory)
    node = dataflow.nodes.get(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Unknown node {node_id!r}")
    return node


def _require_kafka_connection(
    node_id: str, node: NodeDef, feature: str
) -> tuple[str, str]:
    """Common gate for the two live-Kafka endpoints: node must be a kafka-topic with a
    broker/topic to connect to. Returns (broker, topic) or raises HTTPException."""
    if node.type != "kafka-topic":
        raise HTTPException(
            status_code=404,
            detail=f"No {feature} available for node type {node.type!r}",
        )
    connection = node.connection or {}
    broker, topic = connection.get("broker"), connection.get("topic")
    if not broker or not topic:
        raise HTTPException(
            status_code=422,
            detail=f"Node {node_id!r} is missing connection.broker/connection.topic",
        )
    return broker, topic


def create_app(dataflow_dir: Path) -> FastAPI:
    app = FastAPI(title="tkati-dashboard")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/graph")
    def graph() -> dict[str, Any]:
        try:
            return _graph_json(dataflow_dir)
        except DataflowValidationError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e

    @app.get("/api/nodes/{node_id}/snapshot")
    def node_snapshot(node_id: str) -> dict[str, Any]:
        try:
            node = _get_node(dataflow_dir, node_id)
        except DataflowValidationError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        broker, topic = _require_kafka_connection(node_id, node, "live snapshot")

        try:
            events = snapshot.fetch_kafka_snapshot(broker, topic)
        except snapshot.SnapshotError as e:
            raise HTTPException(status_code=502, detail=str(e)) from e

        return {"events": events}

    @app.get("/api/nodes/{node_id}/consumer-lag")
    def node_consumer_lag(node_id: str, group_id: str) -> dict[str, Any]:
        try:
            node = _get_node(dataflow_dir, node_id)
        except DataflowValidationError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        broker, topic = _require_kafka_connection(node_id, node, "consumer lag")

        try:
            return lag.fetch_consumer_lag(broker, topic, group_id)
        except lag.LagError as e:
            raise HTTPException(status_code=502, detail=str(e)) from e

    return app
