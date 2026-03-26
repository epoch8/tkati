"""Serialization / deserialization for arrow-batch and json-per-message encodings."""

from __future__ import annotations

import base64
import io
import json
from datetime import date, datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc


# ---------------------------------------------------------------------------
# arrow-batch
# ---------------------------------------------------------------------------

def deserialize_arrow(data: bytes, schema: pa.Schema) -> pa.RecordBatch:
    """Deserialise an Arrow IPC stream message into a RecordBatch."""
    reader = ipc.open_stream(io.BytesIO(data))
    table = reader.read_all()
    # Cast to declared schema (validates field names / types)
    return table.cast(schema).to_batches()[0] if len(table) else pa.record_batch([], schema=schema)


def serialize_arrow(batch: pa.RecordBatch) -> bytes:
    """Serialise a RecordBatch to Arrow IPC stream bytes."""
    sink = io.BytesIO()
    with ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


# ---------------------------------------------------------------------------
# json-per-message — coercion helpers
# ---------------------------------------------------------------------------

def _coerce_value(value: Any, arrow_type: pa.DataType) -> Any:
    """Coerce a JSON-decoded Python value to match the Arrow type."""
    if value is None:
        return None

    if pa.types.is_integer(arrow_type):
        return int(value)
    if pa.types.is_floating(arrow_type):
        return float(value)
    if pa.types.is_boolean(arrow_type):
        return bool(value)
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return str(value) if not isinstance(value, str) else value
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        if isinstance(value, str):
            return base64.b64decode(value)
        return value
    if pa.types.is_timestamp(arrow_type):
        if isinstance(value, (int, float)):
            return int(value)  # milliseconds since epoch
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        return value
    if pa.types.is_date(arrow_type):
        if isinstance(value, int):
            return value  # days since epoch
        if isinstance(value, str):
            d = date.fromisoformat(value)
            return (d - date(1970, 1, 1)).days
        return value
    return value


def _row_to_pydict(row: dict[str, Any], schema: pa.Schema) -> dict[str, Any]:
    """Filter a JSON row to declared fields and coerce types."""
    result: dict[str, Any] = {}
    for i in range(len(schema)):
        field = schema.field(i)
        raw = row.get(field.name)
        if raw is None and not field.nullable:
            raise ValueError(
                f"Field {field.name!r} is non-nullable but missing from JSON row"
            )
        result[field.name] = _coerce_value(raw, field.type)
    return result


def deserialize_json_messages(
    messages: list[bytes], schema: pa.Schema
) -> pa.RecordBatch:
    """Parse a list of JSON Kafka message bytes into a RecordBatch."""
    rows = [_row_to_pydict(json.loads(m), schema) for m in messages]
    arrays = []
    for i in range(len(schema)):
        field = schema.field(i)
        col = [r[field.name] for r in rows]
        arrays.append(pa.array(col, type=field.type))
    return pa.record_batch(arrays, schema=schema)


def _value_to_json(value: Any, arrow_type: pa.DataType) -> Any:
    """Convert a Python value (from PyArrow) to a JSON-serialisable form."""
    if value is None:
        return None
    if pa.types.is_timestamp(arrow_type):
        # value may be a datetime, pandas Timestamp, or integer
        if hasattr(value, "value"):
            return value.value // 1_000_000  # pandas Timestamp: ns → ms
        if hasattr(value, "timestamp"):
            return int(value.timestamp() * 1000)  # datetime → ms
        return int(value)
    if pa.types.is_date(arrow_type):
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return int(value)
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return base64.b64encode(value).decode()
    return value


def serialize_json_messages(
    batch: pa.RecordBatch,
) -> list[bytes]:
    """Explode a RecordBatch into a list of JSON message bytes (one per row)."""
    schema = batch.schema
    results = []
    pydict = batch.to_pydict()
    for i in range(len(batch)):
        row: dict[str, Any] = {}
        for j in range(len(schema)):
            field = schema.field(j)
            val = pydict[field.name][i]
            row[field.name] = _value_to_json(val, field.type)
        results.append(json.dumps(row).encode())
    return results
