from loguru import logger
from tkati_core.clickhouse.producer import ClickhouseProducer
from tkati_core.consumer import Consumer
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_core.kafka.producer import KafkaProducer
from tkati_core.producer import Producer

from tkati_node_el.settings import AppSettings, InputSettings, OutputSettings


def build_consumer(settings: InputSettings) -> Consumer:
    if settings.type == "kafka":
        return KafkaConsumer.from_input_settings(settings)
    raise ValueError(f"Unsupported input type: {settings.type}")


def build_producer(
    settings: OutputSettings,
    dlq_producer: Producer | None,
    split_factor: int,
) -> Producer:
    if settings.type == "kafka":
        return KafkaProducer.from_topic_settings(settings.topic)
    if settings.type == "clickhouse":
        return ClickhouseProducer.from_output_settings(
            settings=settings,
            dlq_producer=dlq_producer,
            split_factor=split_factor,
        )
    raise ValueError(f"Unsupported output type: {settings.type}")


def run_one_iteration(
    consumer: Consumer,
    producer: Producer,
    settings: AppSettings,
) -> None:
    batch = consumer.read_arrow(
        max_events_to_aggregate=settings.input.consumer.batch_size,
        aggregation_interval_seconds=settings.input.consumer.batch_timeout_sec,
    )
    if batch is None:
        return
    producer.produce_arrow(batch)
    consumer.commit()
    logger.info(f"Produced {len(batch)} rows")


def main() -> None:
    settings = AppSettings()  # type: ignore

    consumer = build_consumer(settings.input)

    dlq_producer: KafkaProducer | None = None
    if settings.dlq is not None:
        dlq_producer = KafkaProducer.from_topic_settings(settings.dlq.topic)

    producer = build_producer(
        settings.output,
        dlq_producer=dlq_producer,
        split_factor=settings.dlq.split_factor if settings.dlq else 10,
    )

    try:
        while True:
            run_one_iteration(consumer, producer, settings)
    finally:
        consumer.close()
        if dlq_producer is not None:
            dlq_producer.close()
