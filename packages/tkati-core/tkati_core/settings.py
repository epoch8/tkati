import os

from loguru import logger
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

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
