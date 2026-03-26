"""Entry point: tkati-node <handler>"""

from __future__ import annotations

import logging
import sys

from tkati_node.env import load_config
from tkati_node.runner import run


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if len(sys.argv) != 2:
        print("Usage: tkati-node <handler>", file=sys.stderr)
        print("  handler: dotted Python path to handler class, e.g. pipelines.clicks.ClickEnricher", file=sys.stderr)
        sys.exit(1)

    handler = sys.argv[1]
    node_config = load_config(handler)
    run(node_config)


if __name__ == "__main__":
    main()
