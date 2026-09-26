"""Kafka consumer utilities for reading messages into PyArrow tables."""

import os
from typing import TYPE_CHECKING, Any

import orjson
import pyarrow as pa
from loguru import logger
from pyarrow import json as pa_json

from tkati_core._native import NativeConsumer, RawBatch
from tkati_core.consumer import ConsumedBatch
from tkati_core.consumer import Consumer as ConsumerBase
from tkati_core.stats import LoopStats
from tkati_core.type_mapping import TYPE_MAPPING

if TYPE_CHECKING:
    from tkati_core.kafka.settings import KafkaInputSettings


def parse_ndjson(
    batch: RawBatch, wire_schema: pa.Schema, internal_schema: pa.Schema
) -> pa.Table:
    """Parse a consumed batch into `internal_schema` with pyarrow's JSON reader.

    The batch's buffer is read in place, and split into blocks small enough
    that every core gets a share of a typical batch: at the reader's default
    1 MiB, a batch of a few thousand messages is a single block and parses on
    one thread. Blocks much smaller than 128 KiB cost more in per-block
    overhead than the parallelism wins back.
    """
    size = len(memoryview(batch))
    block_size = min(
        max(size // (2 * (os.process_cpu_count() or 1)), 128 << 10), 1 << 20
    )
    table = pa_json.read_json(
        pa.BufferReader(batch),
        read_options=pa_json.ReadOptions(block_size=block_size, use_threads=True),
        parse_options=pa_json.ParseOptions(
            explicit_schema=wire_schema,
            unexpected_field_behavior="ignore",
        ),
    )
    return table.cast(internal_schema)


class KafkaConsumer(ConsumerBase):
    """
    A Kafka consumer wrapper that reads messages into PyArrow tables or Python lists.

    This class manages the Kafka consumer lifecycle, topic subscription,
    and provides a convenient interface for reading messages as PyArrow tables
    or plain Python dicts.
    """

    @classmethod
    def from_input_settings(cls, settings: "KafkaInputSettings") -> "KafkaConsumer":
        """
        Construct a KafkaConsumer from a KafkaInputSettings instance.

        Sets enable.auto.commit=False — each batch's offsets are committed
        explicitly via .commit(batch).
        """
        kafka_config: dict[str, str | bool] = {
            "bootstrap.servers": settings.connection.broker,
            "group.id": settings.consumer.group_id,
            "auto.offset.reset": settings.consumer.auto_offset_reset,
            "enable.auto.commit": False,
        }
        return cls(
            kafka_config=kafka_config,
            topic_name=settings.topic.name,
            input_schema=settings.topic.schema,
        )

    def __init__(
        self,
        kafka_config: dict[str, str | bool],
        topic_name: str,
        input_schema: dict[str, str],
    ) -> None:
        """
        Initialize the Kafka consumer with the provided configuration.

        Args:
            kafka_config: Dictionary of Kafka consumer configuration parameters.
                         Common keys include:
                         - 'bootstrap.servers': Kafka broker addresses
                         - 'group.id': Consumer group ID
                         - 'auto.offset.reset': Offset reset behavior
                         - 'enable.auto.commit': Whether to auto-commit offsets
        """
        self.consumer = NativeConsumer(kafka_config, topic_name)
        self.topic_name = topic_name
        self.input_schema = input_schema
        # Batches are numbered as they are read; `_oldest_outstanding` is the
        # one `commit` / `rewind` must be called with next.
        self._next_seq = 0
        self._oldest_outstanding = 0

        # Create PyArrow schemas based on input_schema
        wire_schema_fields = []
        internal_schema_fields = []

        for field_name, field_type in input_schema.items():
            mapping = TYPE_MAPPING.get(field_type)
            if mapping is None:
                raise ValueError(
                    f"Unsupported field type '{field_type}' for field '{field_name}'"
                )
            wire_schema_fields.append(pa.field(field_name, mapping.wire_type))
            internal_schema_fields.append(pa.field(field_name, mapping.internal_type))

        self.wire_schema = pa.schema(wire_schema_fields)
        self.internal_schema = pa.schema(internal_schema_fields)

        logger.info(
            f"Initialized KafkaConsumer with config: {kafka_config} and topic: {topic_name}"
        )

    def _consume_batch(self, timeout: int, num_messages: int) -> RawBatch:
        """
        Consume raw messages from Kafka within the given time and count limits.

        The payloads stay in native memory; consumer errors met along the way
        are logged and skipped.
        """
        # DEBUG rather than INFO: this runs once per loop iteration, which at
        # production throughput is tens of lines a second — enough to bury the
        # periodic perf report that is the actual signal.
        logger.debug(
            f"Consuming events from topic(s): {self.topic_name} for up to {timeout}s or {num_messages} events"
        )

        batch = self.consumer.poll_batch(timeout, num_messages)
        for error in batch.errors:
            logger.info(f"Consumer error: {error}")

        logger.debug(f"Consumed {len(batch)} events")
        return batch

    def _wrap[T](self, data: T, raw: RawBatch) -> ConsumedBatch[T]:
        batch = ConsumedBatch(data=data, offsets=raw.offsets, seq=self._next_seq)
        self._next_seq += 1
        return batch

    # WARNING: This function breaks if any single message is malformed JSON. We may
    # want to enhance it to handle individual message errors more gracefully.
    def read_arrow(
        self,
        timeout: int,
        num_messages: int,
        stats: LoopStats | None = None,
    ) -> ConsumedBatch[pa.Table] | None:
        """
        Read messages from subscribed topics into a PyArrow table.

        Args:
            timeout: Maximum time in seconds to consume messages.
            num_messages: Maximum number of events to consume.
            stats: Optional LoopStats to attribute this read's wall clock to,
                split into `consumer/poll` (waiting on the broker) and
                `consumer/parse` (turning the raw payloads into an Arrow table). These have different
                fixes — batch sizing and broker latency on one side, JSON
                decoding cost on the other — so a caller that only sees a
                single combined figure cannot tell which to chase.

        Returns:
            The parsed events as a PyArrow Table in `.data`, or None if no data
            was consumed.

        Notes:
            - Does NOT commit offsets. Pass the batch to commit() once processed,
              or to rewind() if processing failed.
            - Does NOT subscribe to topics. The consumer must be pre-subscribed.
            - Raises exceptions on JSON parsing errors.
            - Uses permissive parsing that ignores unexpected fields in JSON messages.
        """
        # Discarded when the caller isn't measuring, so the body below never has
        # to branch on `stats is None`. Allocating one per call costs ~1us
        # against a read that takes milliseconds at minimum.
        stats = stats if stats is not None else LoopStats(phases=())

        # Also covers assembling the payloads into one buffer, which happens
        # as they arrive, natively and outside the GIL.
        with stats.phase("consumer/poll"):
            batch = self._consume_batch(timeout, num_messages)
        events_read = len(batch)

        if events_read == 0:
            logger.debug("No data consumed from topic.")
            return None

        with stats.phase("consumer/parse"):
            try:
                if batch.tombstones:
                    raise ValueError(
                        f"{batch.tombstones} of {events_read} messages have no value"
                    )
                table = parse_ndjson(batch, self.wire_schema, self.internal_schema)
                actual_rows = len(table)

                if actual_rows != events_read:
                    logger.warning(
                        f"Row count mismatch: consumed {events_read} messages, but parsed {actual_rows} rows. {events_read - actual_rows} messages may have been skipped."
                    )
                else:
                    logger.debug(
                        f"Successfully parsed {actual_rows} rows matching {events_read} consumed messages"
                    )

            except Exception as e:
                logger.error(f"Failed to parse JSON with PyArrow: {e}")
                raise

        return self._wrap(table, batch)

    def read_pylist(
        self,
        timeout: int,
        num_messages: int,
        stats: LoopStats | None = None,
    ) -> ConsumedBatch[list[dict]] | None:
        """
        Read messages from subscribed topics into a list of dicts.

        Same batching semantics as read_arrow (time + count limits).
        Messages that fail JSON parsing are skipped (logged as errors).

        Args:
            timeout: Maximum time in seconds to consume messages.
            num_messages: Maximum number of events to consume.
            stats: Optional LoopStats, split into `consumer/poll` and
                `consumer/parse` exactly as in read_arrow.

        Returns:
            The parsed event dicts in `.data`, or None if no data was consumed.
            A batch whose messages all failed to parse is None too; its offsets
            are committed once a later batch from the same partitions is.

        Notes:
            - Does NOT commit offsets. Pass the batch to commit() once processed,
              or to rewind() if processing failed.
        """
        stats = stats if stats is not None else LoopStats(phases=())

        with stats.phase("consumer/poll"):
            batch = self._consume_batch(timeout, num_messages)

        if len(batch) == 0:
            logger.debug("No data consumed from topic.")
            return None

        # orjson rather than native parsing: building Python objects needs the
        # GIL either way, and orjson does it in the same pass as parsing — a
        # native parse in parallel followed by a serial build benchmarked
        # slower.
        with stats.phase("consumer/parse"):
            rows = []
            for payload in batch.payloads():
                try:
                    if payload is None:
                        raise ValueError("message has no value")
                    rows.append(orjson.loads(payload))
                except Exception as e:
                    logger.error(
                        f"Error parsing message from topic {self.topic_name}: {e}"
                    )

        logger.debug(f"Successfully parsed {len(rows)} rows")
        return self._wrap(rows, batch) if rows else None

    def _check_oldest(self, batch: ConsumedBatch[Any], action: str) -> None:
        if batch.seq != self._oldest_outstanding:
            raise ValueError(
                f"cannot {action} batch {batch.seq}: batch {self._oldest_outstanding} "
                "is the oldest one not yet committed or rewound"
                if batch.seq > self._oldest_outstanding
                else f"cannot {action} batch {batch.seq}: already committed or rewound"
            )

    def commit(self, batch: ConsumedBatch[Any]) -> None:
        """
        Commit exactly `batch`'s offsets. It must be the oldest batch not yet
        committed or rewound.
        """
        self._check_oldest(batch, "commit")
        self.consumer.commit(batch.offsets)
        self._oldest_outstanding += 1
        logger.debug(f"Committed batch {batch.seq}")

    def rewind(self, batch: ConsumedBatch[Any]) -> None:
        """
        Seek back to where `batch` started, so it is read again. It must be the
        oldest batch not yet committed or rewound; every batch read after it
        is re-read too, and so is dropped from the outstanding ones.
        """
        self._check_oldest(batch, "rewind")
        self.consumer.rewind(batch.offsets)
        self._oldest_outstanding = self._next_seq
        logger.info(f"Rewound batch {batch.seq}; it will be read again")

    def close(self) -> None:
        """
        Close the Kafka consumer and release resources.
        """
        self.consumer.close()
        logger.info("Closed KafkaConsumer")
