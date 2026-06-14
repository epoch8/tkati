from typing import Literal

from pydantic import BaseModel, Field


class KafkaTopicSettings(BaseModel):
    broker: str
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
    topic: KafkaTopicSettings
    consumer: KafkaConsumerSettings


class KafkaOutputSettings(BaseModel):
    topic: KafkaTopicSettings
