"""Kafka producer utilities for writing PyArrow tables as messages."""

from typing import TYPE_CHECKING, Literal

import orjson
import pyarrow as pa
from confluent_kafka import Producer
from loguru import logger

if TYPE_CHECKING:
    from tkati_core.settings import KafkaOutputSettings


class KafkaProducer:
    """
    A Kafka producer wrapper that writes data as messages.

    Supports producing from PyArrow tables/batches or plain Python dicts.

    For Arrow-based production, two serialization formats are controlled by the
    topic's ``format`` setting:
    - ``"json"``: produces one Kafka message per row, serialized with orjson.
    - ``"arrow-batch"``: produces the entire table as a single Arrow IPC message.

    The optional ``key_column`` setting (from ``KafkaTopicSettings``) names the
    column whose value is used as the Kafka message key for each row (JSON format only).
    """

    def __init__(
        self,
        kafka_config: dict[str, str],
        topic_name: str,
        format: Literal["json", "arrow-batch"] = "json",
        key_column: str | None = None,
    ) -> None:
        self.producer = Producer(kafka_config)
        self.topic_name = topic_name
        self.format = format
        self.key_column = key_column
        logger.info(
            f"Initialized KafkaProducer with topic: {topic_name}, format: {format}"
        )

    @classmethod
    def from_output_settings(cls, settings: "KafkaOutputSettings") -> "KafkaProducer":
        """
        Construct a KafkaProducer from a KafkaOutputSettings instance.
        """
        return cls(
            kafka_config={"bootstrap.servers": settings.topic.broker},
            topic_name=settings.topic.name,
            format=settings.topic.format,
            key_column=settings.topic.key_column,
        )

    def produce_arrow(self, data: pa.Table | pa.RecordBatch) -> None:
        """
        Produce data to the configured topic.

        For ``"json"`` format each row becomes a separate Kafka message serialized
        with orjson. If ``key_column`` is set, its value is used as the message key.

        For ``"arrow-batch"`` format the entire table is serialized as a single
        Arrow IPC stream message.
        """
        if self.format == "json":
            self.produce_pylist(data.to_pylist())
        elif self.format == "arrow-batch":
            table = (
                data if isinstance(data, pa.Table) else pa.Table.from_batches([data])
            )
            buf = pa.BufferOutputStream()
            with pa.ipc.new_stream(buf, table.schema) as writer:
                for batch in table.to_batches():
                    writer.write_batch(batch)
            self.producer.produce(self.topic_name, value=buf.getvalue().to_pybytes())

    def produce_pylist(self, rows: list[dict]) -> None:
        """
        Produce a list of dicts to the configured topic as JSON messages.

        Each dict becomes a separate Kafka message serialized with orjson.
        If ``key_column`` is set, its value is used as the Kafka message key.
        """
        for row in rows:
            key = (
                str(row[self.key_column])
                if self.key_column and self.key_column in row
                else None
            )
            self.producer.produce(self.topic_name, value=orjson.dumps(row), key=key)

    def flush(self) -> None:
        """
        Block until all queued messages have been delivered.
        """
        self.producer.flush()
        logger.debug("Flushed KafkaProducer")

    def close(self) -> None:
        """
        Flush pending messages and release resources.
        """
        self.producer.flush()
        logger.info("Closed KafkaProducer")
