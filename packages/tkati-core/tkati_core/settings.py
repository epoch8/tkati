import os
from typing import Annotated

from loguru import logger
from pydantic import Field, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from tkati_core.clickhouse.settings import ClickHouseOutputSettings
from tkati_core.kafka.settings import KafkaInputSettings, KafkaOutputSettings
from tkati_core.metrics import MetricsSettings

SETTINGS_FILE = os.getenv("SETTINGS_FILE", "settings.toml")

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


InputSettings = KafkaInputSettings
OutputSettings = Annotated[
    KafkaOutputSettings | ClickHouseOutputSettings, Field(discriminator="type")
]


class NodeSettings(TomlBaseSettings):
    """The sections every `Node.from_settings` reads. A node subclasses this
    and adds its own.

    `output` is optional because a node may deliver its output itself, e.g.
    through a cloud API client. A node that always has one redeclares it as
    `output: OutputSettings`, so its config fails validation without it.
    """

    input: InputSettings
    output: OutputSettings | None = None
    dlq: OutputSettings | None = None
    metrics: MetricsSettings = MetricsSettings()

    @model_validator(mode="after")
    def _dlq_needs_output(self) -> "NodeSettings":
        # The DLQ only receives rows the output producer rejects, so without
        # an output it would be accepted and then never used.
        if self.dlq is not None and self.output is None:
            raise ValueError(
                "`dlq` is set but `output` is not; the DLQ only takes rows the output rejects"
            )
        return self
