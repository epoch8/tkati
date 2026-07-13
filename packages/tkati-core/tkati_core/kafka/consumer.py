"""Kafka consumer utilities for reading messages into PyArrow tables."""

import time
from io import BytesIO
from typing import TYPE_CHECKING

import orjson
import pyarrow as pa
from confluent_kafka import Consumer
from loguru import logger
from pyarrow import json as pa_json

from tkati_core.consumer import Consumer as ConsumerBase

if TYPE_CHECKING:
    from tkati_core.kafka.settings import KafkaInputSettings


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

        Sets enable.auto.commit=False — offsets must be committed explicitly via .commit().
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
        self.consumer = Consumer(kafka_config)
        self.topic_name = topic_name
        self.input_schema = input_schema

        self.consumer.subscribe([self.topic_name])

        # Create PyArrow schema based on input_schema
        # type -> (parse_type, cast_type)
        type_mapping: dict[str, tuple[pa.DataType, pa.DataType]] = {
            "string": (pa.string(), pa.string()),
            "int32": (pa.int32(), pa.int32()),
            "int64": (pa.int64(), pa.int64()),
            "uint32": (pa.uint32(), pa.uint32()),
            "uint64": (pa.uint64(), pa.uint64()),
            "uint8": (pa.uint8(), pa.uint8()),
            "int": (pa.int32(), pa.int32()),
            "timestamp[ms]": (pa.int64(), pa.timestamp("ms")),
        }

        parse_schema_fields = []
        cast_schema_fields = []

        for field_name, field_type in input_schema.items():
            types = type_mapping.get(field_type)

            if types is not None:
                parse_type, cast_type = types
            else:
                logger.warning(
                    f"Unsupported field type '{field_type}' for field '{field_name}'. Defaulting to string."
                )
                parse_type, cast_type = (pa.string(), pa.string())

            parse_schema_fields.append(pa.field(field_name, parse_type))
            cast_schema_fields.append(pa.field(field_name, cast_type))

        self.parse_schema = pa.schema(parse_schema_fields)
        self.cast_schema = pa.schema(cast_schema_fields)

        logger.info(
            f"Initialized KafkaConsumer with config: {kafka_config} and topic: {topic_name}"
        )

    def _consume_batch(
        self,
        aggregation_interval_seconds: int,
        max_events_to_aggregate: int,
    ) -> tuple[list, int]:
        """
        Consume raw messages from Kafka within the given time and count limits.

        Returns a tuple of (messages, events_read) where messages is a list of
        confluent_kafka Message objects (without errors).
        """
        if self.topic_name:
            logger.info(
                f"Consuming events from topic(s): {self.topic_name} for up to {aggregation_interval_seconds}s or {max_events_to_aggregate} events"
            )
        else:
            logger.info(
                f"Consuming events for up to {aggregation_interval_seconds}s or {max_events_to_aggregate} events"
            )

        start_time = time.time()
        events_read = 0
        poll_timeout = 10
        valid_messages = []

        while events_read < max_events_to_aggregate:
            elapsed = time.time() - start_time
            remaining_time = aggregation_interval_seconds - elapsed

            if remaining_time <= 0:
                logger.info(f"Reached time limit of {aggregation_interval_seconds}s")
                break

            remaining_messages = max_events_to_aggregate - events_read
            batch_timeout = min(poll_timeout, remaining_time)
            messages = self.consumer.consume(
                num_messages=min(remaining_messages, 1_000_000),
                timeout=batch_timeout,
            )

            if not messages:
                continue

            for msg in messages:
                if msg.error():
                    logger.info(f"Consumer error: {msg.error()}")
                    continue
                valid_messages.append(msg)
                events_read += 1

        elapsed_total = time.time() - start_time
        logger.info(f"Consumed {events_read} events in {elapsed_total:.2f}s")
        return valid_messages, events_read

    # WARNING: This function breaks if any single message is malformed JSON. We may
    # want to enhance it to handle individual message errors more gracefully.
    def read_arrow(
        self,
        aggregation_interval_seconds: int,
        max_events_to_aggregate: int,
    ) -> pa.Table | None:
        """
        Read messages from subscribed topics into a PyArrow table.

        Args:
            aggregation_interval_seconds: Maximum time in seconds to consume messages.
            max_events_to_aggregate: Maximum number of events to consume.

        Returns:
            A PyArrow Table containing the parsed events, or None if no data was consumed.

        Notes:
            - Does NOT commit offsets. The caller is responsible for managing consumer lifecycle.
            - Does NOT subscribe to topics. The consumer must be pre-subscribed.
            - Raises exceptions on JSON parsing errors.
            - Uses permissive parsing that ignores unexpected fields in JSON messages.
        """
        valid_messages, events_read = self._consume_batch(
            aggregation_interval_seconds, max_events_to_aggregate
        )

        if events_read == 0:
            logger.info("No data consumed from topic.")
            return None

        buffer = BytesIO()
        for msg in valid_messages:
            buffer.write(msg.value())
            buffer.write(b"\n")
        buffer.seek(0)

        parse_options = pa_json.ParseOptions(
            explicit_schema=self.parse_schema,
            unexpected_field_behavior="ignore",
        )

        try:
            table = pa_json.read_json(buffer, parse_options=parse_options)
            table = table.cast(self.cast_schema)
            actual_rows = len(table)

            if actual_rows != events_read:
                logger.warning(
                    f"Row count mismatch: consumed {events_read} messages, but parsed {actual_rows} rows. {events_read - actual_rows} messages may have been skipped."
                )
            else:
                logger.info(
                    f"Successfully parsed {actual_rows} rows matching {events_read} consumed messages"
                )

        except Exception as e:
            logger.error(f"Failed to parse JSON with PyArrow: {e}")
            raise

        return table

    def read_pylist(
        self,
        aggregation_interval_seconds: int,
        max_events_to_aggregate: int,
    ) -> list[dict] | None:
        """
        Read messages from subscribed topics into a list of dicts.

        Same batching semantics as read_arrow (time + count limits).
        Messages that fail JSON parsing are skipped (logged as errors).

        Returns:
            A list of parsed event dicts, or None if no data was consumed.

        Notes:
            - Does NOT commit offsets. The caller is responsible for managing consumer lifecycle.
        """
        valid_messages, events_read = self._consume_batch(
            aggregation_interval_seconds, max_events_to_aggregate
        )

        if events_read == 0:
            logger.info("No data consumed from topic.")
            return None

        rows = []
        for msg in valid_messages:
            try:
                rows.append(orjson.loads(msg.value()))
            except Exception as e:
                logger.error(f"Error parsing message from topic {msg.topic()}: {e}")

        logger.info(f"Successfully parsed {len(rows)} rows")
        return rows if rows else None

    def commit(self) -> None:
        """
        Commit the current offsets for all subscribed topics.
        """
        self.consumer.commit()
        logger.debug("Committed offsets")

    def close(self) -> None:
        """
        Close the Kafka consumer and release resources.
        """
        self.consumer.close()
        logger.info("Closed KafkaConsumer")
