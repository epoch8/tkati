"""Shared consumer interface, implemented by KafkaConsumer."""

from abc import ABC, abstractmethod

import pyarrow as pa

from tkati_core.settings import InputSettings


class Consumer(ABC):
    """Base class for anything that can act as an input source."""

    @abstractmethod
    def read_arrow(
        self,
        timeout: int,
        num_messages: int,
    ) -> pa.Table | None: ...

    @abstractmethod
    def commit(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


def build_consumer(settings: InputSettings) -> Consumer:
    from tkati_core.kafka.consumer import KafkaConsumer

    match settings:
        case InputSettings():
            return KafkaConsumer.from_input_settings(settings)
        case _:
            raise ValueError(f"Unsupported input settings: {type(settings).__name__}")
