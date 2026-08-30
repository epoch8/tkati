"""Resolve the set of dataflow directories a dashboard instance observes.

A dashboard instance can watch more than one dataflow at once: some directories are named
explicitly on the command line, others are discovered as immediate subdirectories of a
`--flows-root`. Either way, a flow's id is simply its directory's own basename — the same
identity `dataflow.load_dataflow` already gives a `Dataflow.name`.
"""

from collections.abc import Callable
from pathlib import Path

from tkati_dashboard.dataflow import find_fragment_paths


class FlowConfigError(ValueError):
    """The configured dataflow directories/roots don't resolve to an unambiguous flow set."""


def discover_flows_root(root: Path) -> dict[str, Path]:
    """Every immediate subdirectory of `root` that contains at least one dataflow fragment is a
    flow, keyed by its own basename. A subdirectory with no fragments (e.g. a stray `README` or
    `.git`) is silently skipped rather than treated as an error."""
    return {
        path.name: path
        for path in sorted(root.iterdir())
        if path.is_dir() and find_fragment_paths(path)
    }


def make_flow_lister(
    dataflow_dirs: list[Path], flows_roots: list[Path]
) -> Callable[[], dict[str, Path]]:
    """Build a `list_flows()` callable returning the current flow id -> directory map.

    Explicit `dataflow_dirs` are fixed once, at build time. Each `flows_roots` entry is
    re-scanned on every call, so adding or removing a flow subdirectory there is picked up
    without restarting the server — the same "always read fresh, never cached" philosophy
    `load_dataflow` already applies to a single flow's fragments.
    """
    fixed = {directory.name: directory for directory in dataflow_dirs}

    def list_flows() -> dict[str, Path]:
        flows = dict(fixed)
        for root in flows_roots:
            for flow_id, path in discover_flows_root(root).items():
                if flow_id in flows and flows[flow_id] != path:
                    raise FlowConfigError(
                        f"Flow id {flow_id!r} is ambiguous: {flows[flow_id]} and {path} both "
                        "resolve to it — rename one of the directories"
                    )
                flows[flow_id] = path
        return flows

    return list_flows
