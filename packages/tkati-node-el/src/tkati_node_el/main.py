from loguru import logger
from tkati_core import Batch, Node

from tkati_node_el.settings import AppSettings


def run(node: Node) -> None:
    """Write every batch to the output unchanged.

    `done()` waits for delivery before it commits, so delivery is
    at-least-once: a batch is committed only after it is confirmed delivered.
    """
    for event in node:
        if isinstance(event, Batch):
            node.done(event, output=event.data)
            logger.debug(f"Produced {len(event.data)} rows")


def main() -> None:
    settings = AppSettings()
    with Node.from_settings(settings) as node:
        run(node)
