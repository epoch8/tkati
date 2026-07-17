"""Shared producer interface, implemented by KafkaProducer and ClickhouseProducer."""

from abc import ABC, abstractmethod

import pyarrow as pa

from tkati_core.settings import OutputSettings


class Producer(ABC):
    """Base class for anything that can act as a producer target."""

    @abstractmethod
    def produce_arrow(self, data: pa.Table) -> None: ...

    @abstractmethod
    def produce_pylist(self, rows: list[dict]) -> None: ...

    @abstractmethod
    def flush(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


def build_producer(
    settings: OutputSettings,
    dlq_producer: Producer | None = None,
) -> Producer:
    from tkati_core.clickhouse.producer import ClickhouseProducer
    from tkati_core.kafka.producer import KafkaProducer

    match settings:
        case _ if settings.type == "kafka":
            return KafkaProducer.from_output_settings(settings)
        case _ if settings.type == "clickhouse":
            return ClickhouseProducer.from_output_settings(
                settings=settings,
                dlq_producer=dlq_producer,
            )
        case _:
            raise ValueError(f"Unsupported output settings: {type(settings).__name__}")
