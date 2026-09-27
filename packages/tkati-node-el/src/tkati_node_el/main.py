from loguru import logger
from tkati_core import Batch, PipelinedNode

from tkati_node_el.settings import AppSettings


def run(node: PipelinedNode) -> None:
    """Write every batch to the output unchanged.

    `done()` returns while the batch is still in flight; the node commits it
    once it is delivered, in read order. Delivery is at-least-once: a batch
    is committed only after it is confirmed delivered.
    """
    for event in node.consume_arrow():
        if isinstance(event, Batch):
            node.done(event, output_arrow=event.data)
            logger.debug(f"Produced {len(event.data)} rows")


def main() -> None:
    settings = AppSettings()
    with PipelinedNode.from_settings(settings) as node:
        run(node)
