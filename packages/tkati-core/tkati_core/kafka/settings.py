from typing import Literal

from pydantic import BaseModel, Field


class KafkaConnectionSettings(BaseModel):
    broker: str


class KafkaTopicSettings(BaseModel):
    name: str
    schema: dict[str, str] = Field(default_factory=dict)  # type: ignore
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


class KafkaOutputSettings(BaseModel):
    type: Literal["kafka"] = "kafka"
    connection: KafkaConnectionSettings
    topic: KafkaTopicSettings
