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
