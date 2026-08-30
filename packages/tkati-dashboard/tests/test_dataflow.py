import json
from pathlib import Path

import pytest
from tkati_dashboard.dataflow import DataflowValidationError, load_dataflow


def test_load_dataflow_merges_fragments(sample_dataflow_dir: Path) -> None:
    dataflow = load_dataflow(sample_dataflow_dir)

    assert dataflow.name == "sample-dataflow"
    assert set(dataflow.nodes) == {"raw-orders", "dedup", "deduped-orders"}
    assert dataflow.nodes["raw-orders"].type == "kafka-topic"
    assert dataflow.nodes["raw-orders"].schema == {
        "id": "string",
        "time": "timestamp[ms]",
        "amount": "int64",
    }
    assert dataflow.nodes["raw-orders"].connection == {
        "broker": "redpanda:29092",
        "topic": "raw_orders",
    }
    assert dataflow.nodes["dedup"].type == "processing-node"

    assert len(dataflow.edges) == 2
    assert dataflow.edges[0].from_ == "raw-orders"
    assert dataflow.edges[0].to == "dedup"
    assert dataflow.edges[0].consumer == {"group_id": "orders-dedup"}
    assert dataflow.edges[1].from_ == "dedup"
    assert dataflow.edges[1].to == "deduped-orders"


def test_invalid_dataflow_raises(invalid_dataflow_dir: Path) -> None:
    with pytest.raises(DataflowValidationError):
        load_dataflow(invalid_dataflow_dir)


def test_empty_directory_raises(tmp_path: Path) -> None:
    with pytest.raises(DataflowValidationError):
        load_dataflow(tmp_path)


def test_missing_directory_raises(tmp_path: Path) -> None:
    with pytest.raises(DataflowValidationError):
        load_dataflow(tmp_path / "does-not-exist")


def test_source_sink_node_without_schema_is_valid(tmp_path: Path) -> None:
    """Real-world fragments (e.g. discovered from a live cluster without introspecting its
    columns) may not have a schema on hand for a source/sink node — this should still load."""
    (tmp_path / "graph.json").write_text(
        json.dumps(
            {
                "nodes": {
                    "proxy-reboot-requests-cloud-ch": {
                        "type": "clickhouse-table",
                        "name": "proxy_reboot_requests",
                        "connection": {
                            "host": "s6at5d0f40.eu-west-1.aws.clickhouse.cloud",
                            "port": "8443",
                            "database": "soax_stage",
                            "user": "default",
                            "secure": True,
                        },
                    }
                },
                "edges": [],
            }
        )
    )

    dataflow = load_dataflow(tmp_path)

    assert dataflow.nodes["proxy-reboot-requests-cloud-ch"].schema is None


def test_singular_node_fragment_is_valid(tmp_path: Path) -> None:
    """A fragment may declare one node via a top-level "node" object carrying its own "id",
    instead of keying it under "nodes" — e.g. one file per processing node."""
    (tmp_path / "node.json").write_text(
        json.dumps(
            {
                "node": {
                    "id": "stage-eu-reboot-requests-k2ch-cloud-ch",
                    "type": "processing-node",
                    "implementation": "k2ch",
                    "config": {},
                },
                "edges": [
                    {
                        "from": "raw",
                        "to": "stage-eu-reboot-requests-k2ch-cloud-ch",
                        "kind": "stream",
                        "consumer": {"group_id": "g"},
                    },
                    {
                        "from": "stage-eu-reboot-requests-k2ch-cloud-ch",
                        "to": "sink",
                        "kind": "stream",
                    },
                ],
            }
        )
    )
    (tmp_path / "topics.json").write_text(
        json.dumps(
            {
                "nodes": {
                    "raw": {
                        "type": "kafka-topic",
                        "connection": {"broker": "b", "topic": "raw"},
                    },
                    "sink": {
                        "type": "kafka-topic",
                        "connection": {"broker": "b", "topic": "sink"},
                    },
                }
            }
        )
    )

    dataflow = load_dataflow(tmp_path)

    assert set(dataflow.nodes) == {
        "stage-eu-reboot-requests-k2ch-cloud-ch",
        "raw",
        "sink",
    }
    assert (
        dataflow.nodes["stage-eu-reboot-requests-k2ch-cloud-ch"].type
        == "processing-node"
    )
    assert len(dataflow.edges) == 2


def test_singular_node_fragment_without_id_raises(tmp_path: Path) -> None:
    (tmp_path / "node.json").write_text(
        json.dumps({"node": {"type": "processing-node"}})
    )

    with pytest.raises(DataflowValidationError):
        load_dataflow(tmp_path)


@pytest.fixture
def yaml_dataflow_dir() -> Path:
    return Path(__file__).parent / "data" / "yaml-dataflow"


def test_load_dataflow_reads_yaml_fragments(yaml_dataflow_dir: Path) -> None:
    """YAML fragments (.yaml/.yml) merge exactly like .json ones."""
    dataflow = load_dataflow(yaml_dataflow_dir)

    assert dataflow.name == "yaml-dataflow"
    assert set(dataflow.nodes) == {"raw-orders", "dedup", "deduped-orders"}
    assert dataflow.nodes["raw-orders"].schema == {
        "id": "string",
        "time": "timestamp[ms]",
        "amount": "int64",
    }
    assert dataflow.nodes["raw-orders"].connection == {
        "broker": "redpanda:29092",
        "topic": "raw_orders",
    }
    assert len(dataflow.edges) == 2
    assert dataflow.edges[0].consumer == {"group_id": "orders-dedup"}


def test_load_dataflow_merges_json_and_yaml_fragments(tmp_path: Path) -> None:
    (tmp_path / "topics.json").write_text(
        json.dumps(
            {
                "nodes": {
                    "raw": {
                        "type": "kafka-topic",
                        "connection": {"broker": "b", "topic": "raw"},
                    },
                }
            }
        )
    )
    (tmp_path / "rest.yaml").write_text(
        "nodes:\n"
        "  sink:\n"
        "    type: kafka-topic\n"
        "    connection: {broker: b, topic: sink}\n"
        "edges:\n"
        "  - from: raw\n"
        "    to: sink\n"
        "    kind: stream\n"
    )

    dataflow = load_dataflow(tmp_path)

    assert set(dataflow.nodes) == {"raw", "sink"}
    assert len(dataflow.edges) == 1
    assert dataflow.edges[0].from_ == "raw"
    assert dataflow.edges[0].to == "sink"


def test_yaml_fragment_defining_same_node_identically_is_deduped(
    tmp_path: Path,
) -> None:
    node = {"type": "kafka-topic", "connection": {"broker": "b", "topic": "t"}}
    (tmp_path / "a.json").write_text(json.dumps({"nodes": {"shared": node}}))
    (tmp_path / "b.yaml").write_text(
        "nodes:\n  shared:\n    type: kafka-topic\n    connection: {broker: b, topic: t}\n"
    )

    dataflow = load_dataflow(tmp_path)

    assert set(dataflow.nodes) == {"shared"}


def test_empty_yaml_fragment_is_valid(tmp_path: Path) -> None:
    (tmp_path / "empty.yaml").write_text("")
    (tmp_path / "graph.json").write_text(
        json.dumps({"nodes": {"a": {"type": "processing-node"}}, "edges": []})
    )

    dataflow = load_dataflow(tmp_path)

    assert set(dataflow.nodes) == {"a"}


def test_non_object_yaml_fragment_raises(tmp_path: Path) -> None:
    (tmp_path / "bad.yaml").write_text("- just\n- a\n- list\n")

    with pytest.raises(DataflowValidationError):
        load_dataflow(tmp_path)
