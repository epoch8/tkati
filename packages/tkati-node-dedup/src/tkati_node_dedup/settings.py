from pydantic import BaseModel, field_validator
from tkati_core.settings import InputSettings, OutputSettings, TomlBaseSettings


class DedupSettings(BaseModel):
    field: str
    window_hours: int = 3
    bucket_hours: int = 1
    store_dir: str = "./dedup_store"

    @field_validator("window_hours", "bucket_hours")
    @classmethod
    def _positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("must be a positive number of hours")
        return v


class AppSettings(TomlBaseSettings):
    input: InputSettings
    output: OutputSettings
    dlq: OutputSettings | None = None
    dedup: DedupSettings
