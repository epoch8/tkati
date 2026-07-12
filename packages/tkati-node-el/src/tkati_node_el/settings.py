from typing import Annotated, Literal

from pydantic import BaseModel, Field
from tkati_core.clickhouse.settings import ClickHouseOutputSettings
from tkati_core.kafka.settings import KafkaInputSettings, KafkaTopicSettings
from tkati_core.settings import TomlBaseSettings


class KafkaInputConfig(KafkaInputSettings):
    type: Literal["kafka"] = "kafka"


class KafkaOutputConfig(BaseModel):
    type: Literal["kafka"] = "kafka"
    topic: KafkaTopicSettings


class ClickHouseOutputConfig(ClickHouseOutputSettings):
    type: Literal["clickhouse"] = "clickhouse"


InputSettings = Annotated[KafkaInputConfig, Field(discriminator="type")]
OutputSettings = Annotated[
    KafkaOutputConfig | ClickHouseOutputConfig, Field(discriminator="type")
]


class DLQSettings(BaseModel):
    topic: KafkaTopicSettings
    split_factor: int = 10


class AppSettings(TomlBaseSettings):
    input: InputSettings
    output: OutputSettings
    dlq: DLQSettings | None = None
