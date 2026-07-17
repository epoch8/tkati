from tkati_core.settings import TomlBaseSettings
from tkati_core.settings import InputSettings, OutputSettings


class AppSettings(TomlBaseSettings):
    input: InputSettings
    output: OutputSettings
    dlq: OutputSettings | None = None
