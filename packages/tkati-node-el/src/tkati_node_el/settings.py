from pydantic import BaseModel
from tkati_core.node import InputSettings, OutputSettings
from tkati_core.settings import TomlBaseSettings


class DLQSettings(BaseModel):
    output: OutputSettings
    split_factor: int = 10


class AppSettings(TomlBaseSettings):
    input: InputSettings
    output: OutputSettings
    dlq: DLQSettings | None = None
