"""Shared consumer interface, implemented by KafkaConsumer."""

from abc import ABC, abstractmethod

import pyarrow as pa


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
