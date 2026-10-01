from typing import Literal

from pydantic import BaseModel, Field, model_validator

KafkaConfigOverrides = dict[str, str | int | bool]
"""Extra librdkafka properties, passed through to the client verbatim.

Values may be strings, ints or bools; the native layer lowers each to the
string form librdkafka wants (bools become ``true`` / ``false``). Property
names and values are validated by librdkafka itself, when the client is
constructed.
"""

PRODUCER_RESERVED = frozenset({"bootstrap.servers"})
"""Producer properties `KafkaOutputSettings.config` may not set.

Owned by a typed field: ``bootstrap.servers`` comes from
``connection.broker``. Reserved only because something else already sets it —
were the field removed in favour of the passthrough, the property would leave
this set with it.
"""

CONSUMER_RESERVED = frozenset(
    {
        "bootstrap.servers",
        "group.id",
        "auto.offset.reset",
        "enable.auto.commit",
    }
)
"""Consumer properties `KafkaInputSettings.config` may not set.

Owned for correctness: ``enable.auto.commit`` must stay false, because a node
commits each batch explicitly once it is durable and rewinds it on failure.
librdkafka accepts ``enable.auto.commit=true`` silently, so overriding it here
would turn the failure path into silent data loss.

Owned by a typed field, as in `PRODUCER_RESERVED`: ``bootstrap.servers``,
``group.id`` and ``auto.offset.reset``.
"""

_RESERVED_REASONS = {
    "enable.auto.commit": (
        "is managed by tkati and cannot be set here; the node commits each "
        "batch explicitly after it is durable"
    ),
    "bootstrap.servers": "is set from `connection.broker`; set it there",
    "group.id": "is set from `consumer.group_id`; set it there",
    "auto.offset.reset": "is set from `consumer.auto_offset_reset`; set it there",
}


def _reject_reserved(
    config: KafkaConfigOverrides, reserved: frozenset[str]
) -> KafkaConfigOverrides:
    """Raise if `config` names a property tkati sets itself."""
    for key in config:
        if key in reserved:
            raise ValueError(f'config: "{key}" {_RESERVED_REASONS[key]}')
    return config


class KafkaConnectionSettings(BaseModel):
    broker: str


class KafkaTopicSettings(BaseModel):
    name: str
    schema: dict[str, str] = Field(default_factory=dict)
    format: Literal["json", "arrow-batch"] = "json"
    key_column: str | None = None


class KafkaConsumerSettings(BaseModel):
    group_id: str
    batch_size: int = 1000
    batch_timeout_sec: int = 5
    auto_offset_reset: str = "latest"


class KafkaInputSettings(BaseModel):
    type: Literal["kafka"] = "kafka"
    connection: KafkaConnectionSettings
    topic: KafkaTopicSettings
    consumer: KafkaConsumerSettings
    config: KafkaConfigOverrides = Field(default_factory=dict)

    @model_validator(mode="after")
    def _reject_reserved_config(self) -> "KafkaInputSettings":
        _reject_reserved(self.config, CONSUMER_RESERVED)
        return self


class KafkaOutputSettings(BaseModel):
    type: Literal["kafka"] = "kafka"
    connection: KafkaConnectionSettings
    topic: KafkaTopicSettings
    config: KafkaConfigOverrides = Field(default_factory=dict)

    @model_validator(mode="after")
    def _reject_reserved_config(self) -> "KafkaOutputSettings":
        _reject_reserved(self.config, PRODUCER_RESERVED)
        return self
