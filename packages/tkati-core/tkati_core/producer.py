"""Shared producer interface, implemented by KafkaProducer and ClickhouseProducer."""

from abc import ABC, abstractmethod

import pyarrow as pa

from tkati_core.settings import OutputSettings
from tkati_core.stats import PhaseStats

# The phases a producer decomposes its work into, in report order. Exported for
# the same reason as CONSUMER_PHASES: nodes splice them into their own phase
# tuple, so a rename here can't leave a node's column silently reading 0.00s.
PRODUCER_PHASES = ("producer/serialize", "producer/enqueue", "producer/deliver")


class Producer(ABC):
    """Base class for anything that can act as a producer target.

    Every method that takes `stats` attributes its time to the
    `PRODUCER_PHASES` — encoding rows into the wire format (`serialize`),
    handing the encoded messages to the client library (`enqueue`), and waiting
    for the sink to accept them (`deliver`). Implementations that cannot tell
    these apart should record all of it as `producer/deliver`.

    `tag` groups the messages of one produce call, or of several, so a caller
    can wait for exactly those with `wait_delivered(tag)`: the harness tags
    each input batch's output. None (or 0) leaves them untracked.
    """

    @abstractmethod
    def produce_arrow(
        self,
        data: pa.Table,
        stats: PhaseStats | None = None,
        tag: int | None = None,
    ) -> None: ...

    @abstractmethod
    def produce_pylist(
        self,
        rows: list[dict],
        stats: PhaseStats | None = None,
        tag: int | None = None,
    ) -> None: ...

    @abstractmethod
    def wait_delivered(self, tag: int, timeout: float | None = None) -> bool:
        """Wait until every message produced with `tag` is delivered.

        Returns True once they are (at once for a tag nothing was produced
        with), False if `timeout` seconds pass first; None waits as long as it
        takes, 0 only checks. Raises `DeliveryError` as soon as one of them
        has failed, e.g. because librdkafka gave up on it.

        An implementation whose produce calls return only once the sink has
        accepted the data, or raised, returns True straight away.
        """

    @abstractmethod
    def flush(self, stats: PhaseStats | None = None) -> None: ...

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
