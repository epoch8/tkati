from pydantic import BaseModel
from tkati_core.clickhouse.settings import ClickHouseOutputSettings
from tkati_core.kafka.settings import KafkaInputSettings, KafkaTopicSettings
from tkati_core.settings import TomlBaseSettings


class DLQSettings(BaseModel):
    topic: KafkaTopicSettings
    split_factor: int = 10


class AppSettings(TomlBaseSettings):
    input: KafkaInputSettings
    output: ClickHouseOutputSettings
    dlq: DLQSettings | None = None
