from pydantic import BaseModel


class ClickHouseOutputSettings(BaseModel):
    host: str = "localhost"
    port: int = 9000
    user: str = "default"
    password: str = ""
    database: str = "default"
    table: str
    secure: bool = False
