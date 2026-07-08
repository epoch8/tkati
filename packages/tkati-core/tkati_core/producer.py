"""Shared producer interface, implemented by KafkaProducer and ClickhouseProducer."""

from abc import ABC, abstractmethod

import pyarrow as pa


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
