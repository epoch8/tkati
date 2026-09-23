"""Kafka producer utilities for writing PyArrow tables as messages."""

from typing import TYPE_CHECKING, Literal

import orjson
import pyarrow as pa
from loguru import logger

from tkati_core._native import EncodedBatch, NativeProducer, encode_arrow
from tkati_core.producer import Producer as ProducerBase
from tkati_core.stats import LoopStats
from tkati_core.type_mapping import TYPE_MAPPING

if TYPE_CHECKING:
    from tkati_core.kafka.settings import (
        KafkaConnectionSettings,
        KafkaOutputSettings,
        KafkaTopicSettings,
    )


def native_key_type(dtype: pa.DataType) -> bool:
    """Whether ``encode_arrow`` can render keys from a column of this type
    exactly as Python's ``str()`` would."""
    return (
        pa.types.is_integer(dtype)
        or pa.types.is_boolean(dtype)
        or pa.types.is_null(dtype)
        or pa.types.is_string(dtype)
        or pa.types.is_large_string(dtype)
        or pa.types.is_string_view(dtype)
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
    - ``"json"``: produces one Kafka message per row, encoded natively from the
      Arrow columns across all cores.
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
        self.producer = NativeProducer(kafka_config, topic_name)
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

    def produce_arrow(
        self, data: pa.Table | pa.RecordBatch, stats: LoopStats | None = None
    ) -> None:
        """
        Produce data to the configured topic.

        For ``"json"`` format each row becomes a separate Kafka message: a JSON
        object with every column, nulls included. If ``key_column`` is set,
        ``str()`` of its value is used as the message key.

        For ``"arrow-batch"`` format the entire table is serialized as a single
        Arrow IPC stream message.

        When ``stats`` is given, encoding is recorded as ``producer/serialize``
        and handing the messages to librdkafka as ``producer/enqueue``. Neither
        waits on the broker — that is ``flush``'s ``producer/deliver``.
        """
        stats = stats if stats is not None else LoopStats(name="unreported", phases=())

        if self.format == "json":
            with stats.phase("producer/serialize"):
                messages = self._serialize_arrow(
                    _to_wire_table(data, self.wire_type_overrides)
                )
            with stats.phase("producer/enqueue"):
                self.producer.enqueue(messages)
        elif self.format == "arrow-batch":
            with stats.phase("producer/serialize"):
                table = (
                    data
                    if isinstance(data, pa.Table)
                    else pa.Table.from_batches([data])
                )
                buf = pa.BufferOutputStream()
                with pa.ipc.new_stream(buf, table.schema) as writer:
                    for batch in table.to_batches():
                        writer.write_batch(batch)
                messages = EncodedBatch.from_payloads(
                    [(buf.getvalue().to_pybytes(), None)]
                )
            with stats.phase("producer/enqueue"):
                self.producer.enqueue(messages)

    def produce_pylist(self, rows: list[dict], stats: LoopStats | None = None) -> None:
        """
        Produce a list of dicts to the configured topic as JSON messages.

        Each dict becomes a separate Kafka message serialized with orjson.
        If ``key_column`` is set, its value is used as the Kafka message key.
        ``stats`` is split as in ``produce_arrow``.
        """
        stats = stats if stats is not None else LoopStats(name="unreported", phases=())

        with stats.phase("producer/serialize"):
            messages = EncodedBatch.from_payloads(self._serialize_rows(rows))
        with stats.phase("producer/enqueue"):
            self.producer.enqueue(messages)

    def _serialize_arrow(self, data: pa.Table | pa.RecordBatch) -> EncodedBatch:
        """Encode every row natively, in parallel, with no per-row Python
        objects. Keys are derived natively too, unless the key column's type
        is one whose ``str()`` only Python knows how to render (floats,
        timestamps, ...)."""
        key_column = self.key_column
        if key_column is None or key_column not in data.schema.names:
            return encode_arrow(data)
        if native_key_type(data.schema.field(key_column).type):
            return encode_arrow(data, key_column)
        keys = [str(v) for v in data.column(key_column).to_pylist()]
        return encode_arrow(data, keys=keys)

    def _serialize_rows(self, rows: list[dict]) -> list[tuple[bytes, str | None]]:
        """Encode every row up front rather than interleaving it with
        enqueueing, so the two can be timed as separate phases. Stays in
        Python: reading dicts needs the GIL, so there is nothing for native
        code to parallelise."""
        return [
            (
                orjson.dumps(row),
                str(row[self.key_column])
                if self.key_column and self.key_column in row
                else None,
            )
            for row in rows
        ]

    def flush(self, stats: LoopStats | None = None) -> None:
        """
        Block until all queued messages have been delivered.

        Recorded as ``producer/deliver``. librdkafka starts sending in the
        background as soon as messages are enqueued, so this is the *residual*
        wait for broker acks, not the batch's total time on the network.
        """
        stats = stats if stats is not None else LoopStats(name="unreported", phases=())

        with stats.phase("producer/deliver"):
            self.producer.flush()
        logger.debug("Flushed KafkaProducer")

    def close(self) -> None:
        """
        Flush pending messages and release resources.
        """
        self.producer.flush()
        logger.info("Closed KafkaProducer")
