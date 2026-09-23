"""Broker-free benchmark of the Kafka JSON codec paths.

Times the CPU work inside `consumer/parse` and `producer/serialize` on synthetic
payloads, comparing the pure-Python implementation tkati-core used before the
native extension ("legacy") with `tkati_core._native` ("native"). No Kafka is
involved: the payloads are generated in memory and fed straight to the codec.

    uv run python packages/tkati-core/benchmarks/bench_kafka_json.py [--rows 10000 100000]
    RAYON_NUM_THREADS=1 uv run python ...   # native encode, single-threaded
"""

import argparse
import random
import time
from collections.abc import Callable
from functools import partial
from io import BytesIO

import orjson
import pyarrow as pa
from pyarrow import json as pa_json
from tkati_core._native import EncodedBatch, RawBatch, encode_arrow
from tkati_core.kafka.consumer import parse_ndjson
from tkati_core.type_mapping import TYPE_MAPPING

# tkati-node-el's production-shaped input schema.
SCHEMA = {
    "uid": "string",
    "time": "timestamp[ms]",
    "package_id": "int32",
    "user_hash": "string",
    "sdk_hash": "string",
    "conn_type": "string",
    "country": "string",
    "local_ip": "string",
    "frontend_ip": "string",
    "dest_addr": "string",
    "client_ip": "string",
    "traffic_in": "uint32",
    "traffic_out": "uint32",
}

WIRE = pa.schema([pa.field(k, TYPE_MAPPING[v].wire_type) for k, v in SCHEMA.items()])
INTERNAL = pa.schema(
    [pa.field(k, TYPE_MAPPING[v].internal_type) for k, v in SCHEMA.items()]
)


def _ip(rng: random.Random) -> str:
    return ".".join(str(rng.randrange(256)) for _ in range(4))


def make_payloads(n: int, seed: int = 0) -> list[bytes]:
    rng = random.Random(seed)
    return [
        orjson.dumps(
            {
                "uid": f"{rng.getrandbits(128):032x}",
                "time": 1_700_000_000_000 + i,
                "package_id": rng.randrange(10_000),
                "user_hash": f"{rng.getrandbits(64):016x}",
                "sdk_hash": f"{rng.getrandbits(64):016x}",
                "conn_type": rng.choice(["wifi", "cell", "eth"]),
                "country": rng.choice(["US", "DE", "BR", "IN", "JP"]),
                "local_ip": _ip(rng),
                "frontend_ip": _ip(rng),
                "dest_addr": f"{_ip(rng)}:{rng.randrange(65536)}",
                "client_ip": _ip(rng),
                "traffic_in": rng.randrange(2**32),
                "traffic_out": rng.randrange(2**32),
                "unexpected": "ignored by the arrow path",
            }
        )
        for i in range(n)
    ]


# --- legacy: what KafkaConsumer / KafkaProducer did before tkati_core._native ---


def legacy_decode_arrow(payloads: list[bytes]) -> pa.Table:
    buffer = BytesIO()
    for p in payloads:
        buffer.write(p)
        buffer.write(b"\n")
    buffer.seek(0)
    table = pa_json.read_json(
        buffer,
        parse_options=pa_json.ParseOptions(
            explicit_schema=WIRE, unexpected_field_behavior="ignore"
        ),
    )
    return table.cast(INTERNAL)


def legacy_decode_pylist(payloads: list[bytes]) -> list[dict]:
    return [orjson.loads(p) for p in payloads]


def legacy_encode_arrow(table: pa.Table, key_column: str | None) -> list:
    rows = table.cast(WIRE).to_pylist()
    return [
        (
            orjson.dumps(row),
            str(row[key_column]) if key_column and key_column in row else None,
        )
        for row in rows
    ]


# --- native: what KafkaConsumer / KafkaProducer do now ---
#
# Decoding takes a RawBatch built outside the timed region: in production the
# payloads arrive from librdkafka straight into native memory, so copying them
# out of Python `bytes` isn't part of the parse cost.


def native_decode_arrow(batch: RawBatch) -> pa.Table:
    return parse_ndjson(batch, WIRE, INTERNAL)


def native_decode_pylist(batch: RawBatch) -> list[dict]:
    # read_pylist stays on orjson, fed from the native batch.
    return [orjson.loads(p) for p in batch.payloads() if p is not None]


def native_encode_arrow(table: pa.Table, key_column: str) -> EncodedBatch:
    return encode_arrow(table.cast(WIRE), key_column)


def best_of(fn: Callable[[], object], repeat: int) -> float:
    best = float("inf")
    for _ in range(repeat):
        started = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - started)
    return best


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, nargs="+", default=[10_000, 100_000])
    parser.add_argument("--repeat", type=int, default=5)
    args = parser.parse_args()

    for n in args.rows:
        payloads = make_payloads(n)
        raw = RawBatch.from_payloads(payloads)
        table = legacy_decode_arrow(payloads)
        cases: list[tuple[str, Callable[[], object], Callable[[], object]]] = [
            (
                "decode_arrow",
                partial(legacy_decode_arrow, payloads),
                partial(native_decode_arrow, raw),
            ),
            (
                "decode_pylist",
                partial(legacy_decode_pylist, payloads),
                partial(native_decode_pylist, raw),
            ),
            (
                "encode_arrow",
                partial(legacy_encode_arrow, table, "uid"),
                partial(native_encode_arrow, table, "uid"),
            ),
        ]
        for name, legacy, native in cases:
            t_legacy = best_of(legacy, args.repeat)
            t_native = best_of(native, args.repeat)
            print(
                f"{n:>9} rows  {name:<14} legacy {t_legacy * 1e3:9.1f} ms"
                f"   native {t_native * 1e3:9.1f} ms   x{t_legacy / t_native:.1f}"
            )


if __name__ == "__main__":
    main()
