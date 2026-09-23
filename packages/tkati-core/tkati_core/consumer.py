"""Shared consumer interface, implemented by KafkaConsumer."""

from abc import ABC, abstractmethod

import pyarrow as pa

from tkati_core.settings import InputSettings
from tkati_core.stats import LoopStats

# The phases a consumer decomposes its read into, in report order. Exported so
# nodes can splice them into their own phase tuple instead of restating the
# names — a rename here would otherwise leave a node's column silently reading
# 0.00s forever.
CONSUMER_PHASES = ("poll", "parse")


class Consumer(ABC):
    """Base class for anything that can act as an input source."""

    @abstractmethod
    def read_arrow(
        self,
        timeout: int,
        num_messages: int,
        stats: LoopStats | None = None,
    ) -> pa.Table | None:
        """Read a batch into an Arrow table, or None if nothing was available.

        When `stats` is given, the time spent is attributed to the
        `CONSUMER_PHASES` — waiting on the source versus decoding what came
        back. Implementations that cannot tell the two apart should record all
        of it as `poll`.
        """

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
