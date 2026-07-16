from loguru import logger
from tkati_core import Consumer, Producer, build_consumer, build_producer

from tkati_node_el.settings import AppSettings


def run_one_iteration(
    consumer: Consumer,
    producer: Producer,
    settings: AppSettings,
) -> None:
    batch = consumer.read_arrow(
        num_messages=settings.input.consumer.batch_size,
        timeout=settings.input.consumer.batch_timeout_sec,
    )
    if batch is None:
        return
    producer.produce_arrow(batch)
    consumer.commit()
    logger.info(f"Produced {len(batch)} rows")


def main() -> None:
    settings = AppSettings()  # type: ignore

    consumer = build_consumer(settings.input)

    dlq_producer: Producer | None = None
    if settings.dlq is not None:
        dlq_producer = build_producer(settings.dlq)

    producer = build_producer(settings.output, dlq_producer=dlq_producer)

    try:
        while True:
            run_one_iteration(consumer, producer, settings)
    finally:
        consumer.close()
        if dlq_producer is not None:
            dlq_producer.close()
