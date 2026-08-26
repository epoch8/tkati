"""Kafka producer utilities for writing PyArrow tables as messages."""

from typing import TYPE_CHECKING, Literal

import orjson
import pyarrow as pa
from confluent_kafka import Producer
from loguru import logger

from tkati_core.producer import Producer as ProducerBase
from tkati_core.type_mapping import TYPE_MAPPING

if TYPE_CHECKING:
    from tkati_core.kafka.settings import (
        KafkaConnectionSettings,
        KafkaOutputSettings,
        KafkaTopicSettings,
    )


def _to_wire_table(
    data: pa.Table | pa.RecordBatch, wire_type_overrides: dict[str, pa.DataType]
) -> pa.Table | pa.RecordBatch:
    """Cast columns with a declared wire type override — e.g. timestamp[ms] -> int64 epoch —
    so the JSON round trip is symmetric with how the consumer parsed them in, instead of
    guessed from the in-memory pyarrow type."""
    if not wire_type_overrides:
        return data
    new_schema = pa.schema(
        [pa.field(f.name, wire_type_overrides.get(f.name, f.type)) for f in data.schema]
    )
    if new_schema.equals(data.schema):
        return data
    return data.cast(new_schema)


class KafkaProducer(ProducerBase):
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
        output_schema: dict[str, str] | None = None,
    ) -> None:
        self.producer = Producer(kafka_config)
        self.topic_name = topic_name
        self.format = format
        self.key_column = key_column

        self.wire_type_overrides: dict[str, pa.DataType] = {}
        for field_name, field_type in (output_schema or {}).items():
            mapping = TYPE_MAPPING.get(field_type)
            if mapping is None:
                raise ValueError(
                    f"Unsupported field type '{field_type}' for field '{field_name}'"
                )
            self.wire_type_overrides[field_name] = mapping.wire_type

        logger.info(
            f"Initialized KafkaProducer with topic: {topic_name}, format: {format}"
        )

    @classmethod
    def from_topic_settings(
        cls, connection: "KafkaConnectionSettings", topic: "KafkaTopicSettings"
    ) -> "KafkaProducer":
        return cls(
            kafka_config={"bootstrap.servers": connection.broker},
            topic_name=topic.name,
            format=topic.format,
            key_column=topic.key_column,
            output_schema=topic.schema,
        )

    @classmethod
    def from_output_settings(cls, settings: "KafkaOutputSettings") -> "KafkaProducer":
        return cls.from_topic_settings(settings.connection, settings.topic)

    def produce_arrow(self, data: pa.Table | pa.RecordBatch) -> None:
        """
        Produce data to the configured topic.

        For ``"json"`` format each row becomes a separate Kafka message serialized
        with orjson. If ``key_column`` is set, its value is used as the message key.

        For ``"arrow-batch"`` format the entire table is serialized as a single
        Arrow IPC stream message.
        """
        if self.format == "json":
            self.produce_pylist(_to_wire_table(data, self.wire_type_overrides).to_pylist())
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
