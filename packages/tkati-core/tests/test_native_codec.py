"""Broker-free parity tests: the native codec against the pure-Python
implementation it replaced (`to_pylist()` + orjson to encode, one pyarrow
`read_json` over the joined payloads to decode)."""

import datetime as dt
import math
from io import BytesIO

import orjson
import pyarrow as pa
import pytest
from pyarrow import json as pa_json
from tkati_core._native import EncodedBatch, RawBatch, encode_arrow
from tkati_core.kafka.consumer import parse_ndjson
from tkati_core.kafka.producer import KafkaProducer


def legacy_encode(table: pa.Table, key_column: str | None) -> list:
    return [
        (
            orjson.dumps(row),
            str(row[key_column]) if key_column and key_column in row else None,
        )
        for row in table.to_pylist()
    ]


def native_encode(table: pa.Table, key_column: str | None) -> list:
    # Through the producer's own dispatch, so the Python-side key fallback is
    # covered too. Constructing a producer doesn't contact the broker.
    producer = KafkaProducer({"bootstrap.servers": "localhost:1"}, "unused", key_column=key_column)
    batch = producer._serialize_arrow(table)
    return list(zip(batch.payloads(), batch.keys(), strict=True))


PARITY_TABLES = {
    "ints": pa.table({
        "i8": pa.array([-128, 127, None], pa.int8()),
        "i64": pa.array([-(2**63), 2**63 - 1, 0], pa.int64()),
        "u64": pa.array([0, 2**64 - 1, None], pa.uint64()),
        "u32": pa.array([0, 2**32 - 1, 7], pa.uint32()),
    }),
    "strings": pa.table({
        "s": ["plain", 'quote " backslash \\ tab \t newline \n', None],
        "unicode": ["κόσμε", "emoji 🎉", "\u0000 nul and \u001f unit sep"],
        "large": pa.array(["a", None, "c"], pa.large_string()),
    }),
    "bools_and_nulls": pa.table({
        "b": [True, False, None],
        "n": pa.array([None, None, None], pa.null()),
    }),
    "nested": pa.table({
        "l": [[1, 2], [], None],
        "st": [{"x": 1, "y": "a"}, {"x": None, "y": "b"}, None],
    }),
    # Only floats whose repr orjson and the native encoder agree on: they
    # differ on exponent style (1e+16 vs 1.0e16), and float32 is written at
    # its own precision rather than widened to float64 first.
    "ascii_floats": pa.table({"f": [0.5, -1.25, 1e3]}),
    # Timestamps without a wire override go out as ISO strings, which must
    # keep datetime.isoformat()'s shape: microseconds only when non-zero, and
    # a +HH:MM offset rather than Z.
    "timestamps": pa.table({
        "ms": pa.array([1_700_000_000_123, 1_700_000_000_000, None], pa.timestamp("ms")),
        "s": pa.array([0, -1, 86_400], pa.timestamp("s")),
        "us_utc": pa.array([1_700_000_000_123_456, 1, None], pa.timestamp("us", tz="UTC")),
        # Either side of a DST change, so the offset must be per value.
        "berlin": pa.array(
            [1_711_846_799_000, 1_711_846_800_000, None], pa.timestamp("ms", tz="Europe/Berlin")
        ),
        "fixed": pa.array([1_700_000_000_000, None, 0], pa.timestamp("ms", tz="-03:30")),
    }),
    "nested_timestamp": pa.table({
        "st": pa.array(
            [{"t": dt.datetime(2024, 1, 2, 3, 4, 5, 6)}, {"t": None}, None],
            pa.struct([("t", pa.timestamp("us"))]),
        ),
    }),
}


@pytest.mark.parametrize("name", PARITY_TABLES)
def test_encode_matches_legacy_byte_for_byte(name: str):
    table = PARITY_TABLES[name]
    assert native_encode(table, None) == legacy_encode(table, None)


@pytest.mark.parametrize(
    ("column", "values", "dtype"),
    [
        ("k", ["a", None, "é"], pa.string()),
        ("k", [1, -2, None], pa.int32()),
        ("k", [2**64 - 1, 0, None], pa.uint64()),
        ("k", [True, False, None], pa.bool_()),
        # Not native key types: keys come from Python's str() instead.
        ("k", [1.5, 2.0, None], pa.float64()),
        ("k", [dt.date(2024, 1, 2), None, dt.date(1970, 1, 1)], pa.date32()),
    ],
)
def test_keys_match_legacy(column: str, values: list, dtype: pa.DataType):
    table = pa.table({column: pa.array(values, dtype), "v": [1, 2, 3]})
    assert native_encode(table, column) == legacy_encode(table, column)


def test_key_column_absent_means_no_key():
    table = pa.table({"v": [1, 2]})
    assert [k for _, k in native_encode(table, "missing")] == [None, None]


def test_encode_accepts_record_batches_and_chunked_tables():
    table = pa.concat_tables([pa.table({"n": [1, 2]}), pa.table({"n": [3]})])
    assert table.column("n").num_chunks == 2
    assert encode_arrow(table).payloads() == [b'{"n":1}', b'{"n":2}', b'{"n":3}']
    assert encode_arrow(table.to_batches()[1]).payloads() == [b'{"n":3}']


def test_encode_preserves_order_across_parallel_tasks():
    n = 50_000
    batch = encode_arrow(pa.table({"i": pa.array(range(n), pa.int64())}), "i")
    assert [orjson.loads(p)["i"] for p in batch.payloads()] == list(range(n))
    assert batch.keys() == [str(i) for i in range(n)]


def test_float_special_values_are_null_like_orjson():
    """orjson writes NaN and ±inf as null; so must the native encoder, or the
    consumer on the other end would get invalid JSON."""
    table = pa.table({"f": [math.nan, math.inf, -math.inf]})
    assert native_encode(table, None) == legacy_encode(table, None)


def test_timestamp_with_wire_override_encodes_as_epoch_int():
    """With a `timestamp[ms]` output schema the column goes out as an int, as
    before; the cast happens in Python ahead of the native encoder."""
    table = pa.table({"ts": pa.array([1_700_000_000_123], pa.int64())}).cast(
        pa.schema([pa.field("ts", pa.timestamp("ms"))])
    )
    from tkati_core.kafka.producer import _to_wire_table

    wire = _to_wire_table(table, {"ts": pa.int64()})
    assert encode_arrow(wire).payloads() == [b'{"ts":1700000000123}']


def test_encoded_batch_from_payloads_round_trips():
    batch = EncodedBatch.from_payloads([(b"x", None), (b"", "k")])
    assert len(batch) == 2
    assert batch.payloads() == [b"x", b""]
    assert batch.keys() == [None, "k"]


# --- decode ---

WIRE = pa.schema([("id", pa.string()), ("value", pa.int64()), ("ts", pa.int64())])
INTERNAL = pa.schema([("id", pa.string()), ("value", pa.int64()), ("ts", pa.timestamp("ms"))])


def legacy_decode(payloads: list[bytes]) -> pa.Table:
    buffer = BytesIO(b"".join(p + b"\n" for p in payloads))
    return pa_json.read_json(
        buffer,
        parse_options=pa_json.ParseOptions(explicit_schema=WIRE, unexpected_field_behavior="ignore"),
    ).cast(INTERNAL)


def test_decode_matches_legacy_across_many_blocks():
    """Big enough that parse_ndjson splits it into many blocks parsed on
    different threads; rows must come back complete and in order."""
    payloads = [
        orjson.dumps({"id": f"k{i}", "value": i, "ts": 1_700_000_000_000 + i, "extra": "x" * (i % 50)})
        for i in range(60_000)
    ]
    native = parse_ndjson(RawBatch.from_payloads(payloads), WIRE, INTERNAL)
    assert native.equals(legacy_decode(payloads))


def test_decode_missing_fields_are_null_and_unknown_ignored():
    payloads = [b'{"id":"a"}', b'{"value":2,"surprise":{"nested":[1]}}']
    native = parse_ndjson(RawBatch.from_payloads(payloads), WIRE, INTERNAL)
    assert native.to_pylist() == [
        {"id": "a", "value": None, "ts": None},
        {"id": None, "value": 2, "ts": None},
    ]


def test_decode_raises_on_malformed_json():
    with pytest.raises(pa.ArrowInvalid):
        parse_ndjson(RawBatch.from_payloads([b'{"id":"a"}', b'{"id":']), WIRE, INTERNAL)


def test_raw_batch_buffer_is_ndjson_without_tombstones():
    batch = RawBatch.from_payloads([b'{"a":1}', None, b"{}"])
    assert (len(batch), batch.tombstones) == (3, 1)
    assert bytes(memoryview(batch)) == b'{"a":1}\n{}\n'
    assert batch.payloads() == [b'{"a":1}', None, b"{}"]


def test_raw_batch_buffer_is_read_only():
    view = memoryview(RawBatch.from_payloads([b"{}"]))
    assert view.readonly
    with pytest.raises(TypeError):
        view[0] = 0
