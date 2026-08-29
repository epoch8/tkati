import argparse
from pathlib import Path

import uvicorn

from tkati_dashboard.app import create_app


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="tkati-dashboard",
        description="Serve a graph view of a serialized tkati dataflow directory",
    )
    parser.add_argument(
        "dataflow_dir",
        type=Path,
        help="Path to the dataflow directory (a directory of *.json fragments)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if not args.dataflow_dir.is_dir():
        parser.error(f"{args.dataflow_dir} is not a directory")
    if not any(args.dataflow_dir.glob("*.json")):
        parser.error(f"{args.dataflow_dir} contains no dataflow fragments (*.json)")

    app = create_app(args.dataflow_dir)
    uvicorn.run(app, host=args.host, port=args.port)
