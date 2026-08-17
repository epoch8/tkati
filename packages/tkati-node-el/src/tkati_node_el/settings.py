from tkati_core.settings import InputSettings, OutputSettings, TomlBaseSettings


class AppSettings(TomlBaseSettings):
    input: InputSettings
    output: OutputSettings
    dlq: OutputSettings | None = None
