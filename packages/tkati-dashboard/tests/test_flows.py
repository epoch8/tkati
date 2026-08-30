from pathlib import Path

import pytest
from tkati_dashboard.flows import FlowConfigError, discover_flows_root, make_flow_lister

DATA_DIR = Path(__file__).parent / "data"


def test_discover_flows_root_finds_every_fragment_subdirectory() -> None:
    flows = discover_flows_root(DATA_DIR)

    assert flows["sample-dataflow"] == DATA_DIR / "sample-dataflow"
    assert flows["yaml-dataflow"] == DATA_DIR / "yaml-dataflow"
    assert flows["invalid-dangling-edge"] == DATA_DIR / "invalid-dangling-edge"


def test_discover_flows_root_skips_subdirectory_without_fragments(
    tmp_path: Path,
) -> None:
    (tmp_path / "has-fragments").mkdir()
    (tmp_path / "has-fragments" / "nodes.json").write_text("{}")
    (tmp_path / "empty").mkdir()

    flows = discover_flows_root(tmp_path)

    assert set(flows) == {"has-fragments"}


def test_make_flow_lister_combines_explicit_dirs_and_flows_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "flow-a").mkdir()
    (root / "flow-a" / "nodes.json").write_text("{}")
    explicit_dir = tmp_path / "flow-b"
    explicit_dir.mkdir()
    (explicit_dir / "nodes.json").write_text("{}")

    list_flows = make_flow_lister([explicit_dir], [root])

    assert list_flows() == {"flow-a": root / "flow-a", "flow-b": explicit_dir}


def test_make_flow_lister_rescans_flows_root_on_every_call(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    list_flows = make_flow_lister([], [root])

    assert list_flows() == {}

    (root / "flow-a").mkdir()
    (root / "flow-a" / "nodes.json").write_text("{}")

    assert list_flows() == {"flow-a": root / "flow-a"}


def test_make_flow_lister_raises_on_id_collision(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "flow-a").mkdir()
    (root / "flow-a" / "nodes.json").write_text("{}")
    colliding_dir = tmp_path / "flow-a"
    colliding_dir.mkdir()
    (colliding_dir / "nodes.json").write_text("{}")

    list_flows = make_flow_lister([colliding_dir], [root])

    with pytest.raises(FlowConfigError, match="flow-a"):
        list_flows()
