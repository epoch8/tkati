"""Load and validate a serialized tkati dataflow directory.

See docs/dataflow-serialization.md for the format this module implements: a directory of JSON
fragments merged into one graph of nodes and edges. There is no manifest file — every `*.json`
file directly inside the directory is a fragment.
"""

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from tkati_core.type_mapping import TYPE_MAPPING

# Node types that represent data at rest (as opposed to a processing step) and therefore require
# a `schema`. Kept as a heuristic, not a closed registry: an unrecognized type is still accepted,
# it just isn't schema-checked.
SOURCE_SINK_TYPES = {"kafka-topic", "clickhouse-table"}


class DataflowValidationError(ValueError):
    """A serialized dataflow directory failed validation."""


class NodeDef(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str
    name: str | None = None
    schema: dict[str, str] | None = None
    connection: dict[str, Any] | None = None
    config: dict[str, Any] | None = None


class EdgeDef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    to: str
    kind: str = "stream"
    consumer: dict[str, Any] | None = None


class Dataflow(BaseModel):
    name: str
    nodes: dict[str, NodeDef]
    edges: list[EdgeDef]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError as e:
        raise DataflowValidationError(f"Missing dataflow file: {path}") from e
    except json.JSONDecodeError as e:
        raise DataflowValidationError(f"Invalid JSON in {path}: {e}") from e


def _validate_node(node_id: str, node: NodeDef) -> None:
    # `schema` is optional even for source/sink nodes: a real-world fragment (e.g. one written
    # by hand, or discovered from a live cluster without introspecting its columns) may not
    # have one on hand. When it is present, its field types are still checked.
    if node.schema is not None:
        for field_name, field_type in node.schema.items():
            if field_type not in TYPE_MAPPING:
                raise DataflowValidationError(
                    f"Node {node_id!r} field {field_name!r} has unknown schema type {field_type!r}"
                )


def _merge_node(
    nodes: dict[str, NodeDef],
    node_sources: dict[str, str],
    fragment_name: str,
    node_id: str,
    raw_node: dict[str, Any],
) -> None:
    node = NodeDef.model_validate(raw_node)
    if node_id in nodes:
        if nodes[node_id] != node:
            raise DataflowValidationError(
                f"Node {node_id!r} is defined differently in "
                f"{node_sources[node_id]!r} and {fragment_name!r}"
            )
        return
    nodes[node_id] = node
    node_sources[node_id] = fragment_name


def load_dataflow(directory: Path) -> Dataflow:
    """Read every `*.json` fragment directly inside `directory`, merge, and validate them.

    There is no manifest: any JSON file in the directory is a fragment contributing to the
    graph. The dataflow's name is the directory's own name.
    """
    if not directory.is_dir():
        raise DataflowValidationError(f"Not a directory: {directory}")

    fragment_paths = sorted(directory.glob("*.json"))
    if not fragment_paths:
        raise DataflowValidationError(
            f"No dataflow fragments (*.json) found in {directory}"
        )

    nodes: dict[str, NodeDef] = {}
    node_sources: dict[
        str, str
    ] = {}  # node id -> fragment it was first seen in, for error messages
    edges: list[EdgeDef] = []

    for fragment_path in fragment_paths:
        fragment = _read_json(fragment_path)

        for node_id, raw_node in fragment.get("nodes", {}).items():
            _merge_node(nodes, node_sources, fragment_path.name, node_id, raw_node)

        # A fragment may also declare a single node via a top-level "node" object carrying
        # its own "id", instead of keying it under "nodes" — e.g. one file per node.
        if "node" in fragment:
            raw_node = fragment["node"]
            node_id = raw_node.get("id")
            if not node_id:
                raise DataflowValidationError(
                    f"{fragment_path.name!r} has a 'node' object with no 'id'"
                )
            _merge_node(nodes, node_sources, fragment_path.name, node_id, raw_node)

        for raw_edge in fragment.get("edges", []):
            edges.append(EdgeDef.model_validate(raw_edge))

    for node_id, node in nodes.items():
        _validate_node(node_id, node)

    for edge in edges:
        for endpoint in (edge.from_, edge.to):
            if endpoint not in nodes:
                raise DataflowValidationError(
                    f"Edge {edge.from_!r} -> {edge.to!r} references unknown node {endpoint!r}"
                )

    return Dataflow(name=directory.name, nodes=nodes, edges=edges)
