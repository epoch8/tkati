"""Generic node input/output: settings unions and factories, keyed off settings class."""

from typing import Annotated

from pydantic import Field

from tkati_core.clickhouse.producer import ClickhouseProducer
from tkati_core.clickhouse.settings import ClickHouseOutputSettings
from tkati_core.consumer import Consumer
from tkati_core.kafka.consumer import KafkaConsumer
from tkati_core.kafka.producer import KafkaProducer
from tkati_core.kafka.settings import KafkaInputSettings, KafkaOutputSettings
from tkati_core.producer import Producer

InputSettings = KafkaInputSettings
OutputSettings = Annotated[
    KafkaOutputSettings | ClickHouseOutputSettings, Field(discriminator="type")
]


def build_consumer(settings: InputSettings) -> Consumer:
    match settings:
        case KafkaInputSettings():
            return KafkaConsumer.from_input_settings(settings)
        case _:
            raise ValueError(f"Unsupported input settings: {type(settings).__name__}")


def build_producer(
    settings: OutputSettings,
    dlq_producer: Producer | None = None,
) -> Producer:
    match settings:
        case KafkaOutputSettings():
            return KafkaProducer.from_output_settings(settings)
        case ClickHouseOutputSettings():
            return ClickhouseProducer.from_output_settings(
                settings=settings,
                dlq_producer=dlq_producer,
            )
        case _:
            raise ValueError(f"Unsupported output settings: {type(settings).__name__}")
