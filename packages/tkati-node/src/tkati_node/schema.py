"""Schema pydantic models and Arrow conversion."""

from __future__ import annotations

import re

import pyarrow as pa
from pydantic import BaseModel, RootModel, field_validator


def _parse_type(type_str: str) -> pa.DataType:
    type_str = type_str.strip()
    if type_str == "utf8":
        return pa.utf8()
    if type_str == "int32":
        return pa.int32()
    if type_str == "int64":
        return pa.int64()
    if type_str == "float32":
        return pa.float32()
    if type_str == "float64":
        return pa.float64()
    if type_str == "bool":
        return pa.bool_()
    if type_str == "date32":
        return pa.date32()
    if type_str == "timestamp[ms, UTC]":
        return pa.timestamp("ms", tz="UTC")
    if type_str == "binary":
        return pa.large_binary()
    m = re.fullmatch(r"list<(.+)>", type_str)
    if m:
        return pa.list_(_parse_type(m.group(1)))
    raise ValueError(f"Unknown tkati type string: {type_str!r}")


class FieldDef(BaseModel):
    name: str
    type: str
    nullable: bool = True

    @field_validator("type")
    @classmethod
    def _valid_type(cls, v: str) -> str:
        _parse_type(v)  # raises if unknown
        return v


class SchemaDef(RootModel[list[FieldDef]]):
    def to_arrow(self) -> pa.Schema:
        return pa.schema([
            pa.field(f.name, _parse_type(f.type), nullable=f.nullable)
            for f in self.root
        ])
