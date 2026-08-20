"""Shared schema-type-string -> pyarrow-type mapping for Kafka JSON (de)serialization."""

from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class FieldTypeMapping:
    wire_type: pa.DataType
    """The pyarrow type used to parse/produce the JSON representation on the wire."""
    internal_type: pa.DataType
    """The pyarrow type used for the in-memory/Arrow representation."""


TYPE_MAPPING: dict[str, FieldTypeMapping] = {
    "string": FieldTypeMapping(pa.string(), pa.string()),
    "int32": FieldTypeMapping(pa.int32(), pa.int32()),
    "int64": FieldTypeMapping(pa.int64(), pa.int64()),
    "uint32": FieldTypeMapping(pa.uint32(), pa.uint32()),
    "uint64": FieldTypeMapping(pa.uint64(), pa.uint64()),
    "uint8": FieldTypeMapping(pa.uint8(), pa.uint8()),
    "int": FieldTypeMapping(pa.int32(), pa.int32()),
    "timestamp[ms]": FieldTypeMapping(pa.int64(), pa.timestamp("ms")),
}
