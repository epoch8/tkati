import argparse
from pathlib import Path

import uvicorn

from tkati_dashboard.app import create_app
from tkati_dashboard.dataflow import FRAGMENT_GLOBS, find_fragment_paths
from tkati_dashboard.flows import FlowConfigError, make_flow_lister


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="tkati-dashboard",
        description="Serve a graph view of one or more serialized tkati dataflow directories",
    )
    parser.add_argument(
        "dataflow_dir",
        type=Path,
        nargs="*",
        default=[],
        help=(
            "Path to a dataflow directory (a directory of *.json/*.yaml/*.yml fragments). Pass "
            "more than one to observe multiple flows from one dashboard; see --flows-root to "
            "auto-discover a whole directory of them instead of naming each one"
        ),
    )
    parser.add_argument(
        "--flows-root",
        type=Path,
        action="append",
        default=[],
        metavar="DIR",
        help=(
            "Treat every immediate subdirectory of DIR that contains dataflow fragments as its "
            "own flow (id = subdirectory name). Re-scanned on every request, so adding or "
            "removing a flow directory there is picked up without restarting. Repeatable."
        ),
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if not args.dataflow_dir and not args.flows_root:
        parser.error("pass at least one dataflow_dir or --flows-root")

    for directory in args.dataflow_dir:
        if not directory.is_dir():
            parser.error(f"{directory} is not a directory")
        if not find_fragment_paths(directory):
            parser.error(
                f"{directory} contains no dataflow fragments ({'/'.join(FRAGMENT_GLOBS)})"
            )

    seen: dict[str, Path] = {}
    for directory in args.dataflow_dir:
        flow_id = directory.name
        if flow_id in seen and seen[flow_id] != directory:
            parser.error(
                f"flow id {flow_id!r} is ambiguous: {seen[flow_id]} and {directory} both have "
                "this directory name — rename one of them"
            )
        seen[flow_id] = directory

    for root in args.flows_root:
        if not root.is_dir():
            parser.error(f"{root} is not a directory")

    list_flows = make_flow_lister(args.dataflow_dir, args.flows_root)
    try:
        if not list_flows():
            parser.error("no flows found: check the given dataflow_dir(s)/--flows-root")
    except FlowConfigError as e:
        parser.error(str(e))

    app = create_app(list_flows)
    uvicorn.run(app, host=args.host, port=args.port)
