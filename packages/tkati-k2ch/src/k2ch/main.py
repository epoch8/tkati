from loguru import logger
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_core.kafka.producer import KafkaProducer

from tkati_core.clickhouse.producer import ClickhouseProducer
from k2ch.settings import AppSettings


def run_one_iteration(
    kafka_consumer: KafkaConsumer,
    ch_producer: ClickhouseProducer,
    settings: AppSettings,
) -> None:
    batch = kafka_consumer.read_arrow(
        max_events_to_aggregate=settings.input.consumer.batch_size,
        aggregation_interval_seconds=settings.input.consumer.batch_timeout_sec,
    )
    if batch is None:
        return
    ch_producer.produce_arrow(batch)
    kafka_consumer.commit()
    logger.info(f"Inserted {len(batch)} rows to {ch_producer._table}")


def main() -> None:
    settings = AppSettings()  # type: ignore

    kafka_consumer = KafkaConsumer.from_input_settings(settings.input)

    dlq_producer: KafkaProducer | None = None
    if settings.dlq is not None:
        dlq_producer = KafkaProducer.from_topic_settings(settings.dlq.topic)

    ch_producer = ClickhouseProducer.from_output_settings(
        settings=settings.output,
        dlq_producer=dlq_producer,
        split_factor=settings.dlq.split_factor if settings.dlq else 10,
    )

    try:
        while True:
            run_one_iteration(kafka_consumer, ch_producer, settings)
    finally:
        kafka_consumer.close()
        if dlq_producer is not None:
            dlq_producer.close()
