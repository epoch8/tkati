from tkati_core.node import InputSettings, OutputSettings
from tkati_core.settings import TomlBaseSettings


class AppSettings(TomlBaseSettings):
    input: InputSettings
    output: OutputSettings
    dlq: OutputSettings | None = None
