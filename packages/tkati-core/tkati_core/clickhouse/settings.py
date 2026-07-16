from typing import Literal

from pydantic import BaseModel


class ClickHouseConnectionSettings(BaseModel):
    host: str = "localhost"
    port: int = 9000
    user: str = "default"
    password: str = ""
    secure: bool = False


class ClickHouseTableSettings(BaseModel):
    database: str = "default"
    name: str


class ClickHouseOutputSettings(BaseModel):
    type: Literal["clickhouse"] = "clickhouse"
    connection: ClickHouseConnectionSettings
    table: ClickHouseTableSettings
    dlq_split_factor: int = 10
