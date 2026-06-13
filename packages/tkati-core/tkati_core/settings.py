import os
from typing import Literal

from loguru import logger
from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

SETTINGS_FILE = os.getenv("SETTINGS_FILE", "settings.toml")


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


logger.info(f"Using settings file: {os.path.abspath(SETTINGS_FILE)}")


class TomlBaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        toml_file=SETTINGS_FILE,
        env_file=".env",
        extra="ignore",
        env_nested_delimiter="__",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
            TomlConfigSettingsSource(settings_cls),
        )
