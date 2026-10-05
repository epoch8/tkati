from loguru import logger
from tkati_core import Batch, PipelinedNode
from tkati_core.producer import PRODUCER_PHASES

from tkati_node_el.settings import AppSettings
from tkati_node_el.transform import Transform, load_transform

# `PipelinedNode`'s default phases, with the transform where it runs: after
# the loop takes the batch, before the producer encodes it. Only used when a
# transform is configured, so other deployments' perf line doesn't change.
TRANSFORM_PHASES = (
    "wait/input",
    "transform",
    *PRODUCER_PHASES,
    "wait/in-flight",
    "commit",
)


def run(node: PipelinedNode, transform: Transform | None = None) -> None:
    """Write every batch to the output, through `transform` if there is one.

    `done()` returns while the batch is still in flight; the node commits it
    once it is delivered, in read order. Delivery is at-least-once: a batch
    is committed only after it is confirmed delivered. An exception from
    `transform` propagates, and the node rewinds the batch.
    """
    for event in node.consume_arrow():
        if isinstance(event, Batch):
            output = event.data
            if transform is not None:
                with node.phase("transform"):
                    output = transform(output)
            node.done(event, output_arrow=output)
            logger.debug(f"Produced {len(output)} rows")


def main() -> None:
    settings = AppSettings()
    # Before connecting to anything, so broken code fails fast.
    transform = (
        load_transform(settings.transform) if settings.transform is not None else None
    )
    phases = TRANSFORM_PHASES if transform is not None else None
    with PipelinedNode.from_settings(settings, phases=phases) as node:
        run(node, transform)
