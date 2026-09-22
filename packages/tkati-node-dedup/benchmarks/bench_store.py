#!/usr/bin/env python
r"""A/B benchmark for BucketedDedupStore's RocksDB tuning.

Lives outside `src/` so it isn't packaged into the wheel.

The measurement has to happen in a *fresh process* from the one that wrote the
data, otherwise everything is still sitting in the memtable, no SST has been
read, and every table-level option (bloom filter, block cache, compression)
looks like it does nothing. `--phase all` handles that by re-invoking this
script twice as subprocesses.

Read and write throughput are reported separately on purpose: bloom filters
only help the read side, and the write side is the one that scales with the
input rate when duplicates are rare.

Examples
--------
Baseline, reproducing RocksDB's defaults, i.e. how this store behaved before
any tuning::

    python bench_store.py --keys 2000000 \\
        --no-point-lookup --cache-mb 8 --memtable-bloom-ratio 0 \\
        --compression snappy

Current defaults::

    python bench_store.py --keys 2000000
"""

import argparse
import hashlib
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

# Import the real store so the benchmark can never drift from production.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tkati_node_dedup.settings import RocksDBSettings
from tkati_node_dedup.store import BucketedDedupStore


def make_key(i: int, key_len: int) -> bytes:
    """Deterministic, high-entropy key for index `i`.

    High entropy matters: real dedup keys are UUID-ish, and low-entropy keys
    would flatter both the compression and the block-cache numbers.
    """
    digest = hashlib.blake2b(str(i).encode(), digest_size=32).hexdigest().encode()
    return (digest * (key_len // len(digest) + 1))[:key_len]


def build_tuning(args: argparse.Namespace) -> RocksDBSettings:
    return RocksDBSettings(
        point_lookup_optimized=args.point_lookup,
        memtable_bloom_ratio=args.memtable_bloom_ratio,
        block_cache_mb=args.cache_mb,
        write_buffer_mb=args.write_buffer_mb,
        disable_auto_compactions=args.disable_auto_compactions,
        compression=args.compression,
        disable_wal=args.disable_wal,
        enable_statistics=args.enable_statistics,
    )


def open_store(args: argparse.Namespace) -> BucketedDedupStore:
    return BucketedDedupStore(
        args.dir,
        window_hours=args.buckets,
        bucket_hours=1,
        tuning=build_tuning(args),
    )


def phase_populate(args: argparse.Namespace) -> None:
    store = open_store(args)
    batch = args.batch
    # Timed here rather than inside the store: production code carries no
    # instrumentation, so the benchmark brackets the calls it cares about.
    # Only add_many() is timed, so generating the keys doesn't pollute the
    # "inside RocksDB" figure.
    write_sec = 0.0
    started = time.perf_counter()
    for start in range(0, args.keys, batch):
        keys = [
            make_key(i, args.key_len)
            for i in range(start, min(start + batch, args.keys))
        ]
        write_started = time.perf_counter()
        store.add_many(keys)
        write_sec += time.perf_counter() - write_started
    elapsed = time.perf_counter() - started

    print(
        f"WRITE: {args.keys} keys in {elapsed:.1f}s "
        f"({args.keys / elapsed / 1000:.0f}k keys/s, "
        f"{write_sec / args.keys * 1e6:.2f} µs/key inside RocksDB, "
        f"{(elapsed - write_sec) / args.keys * 1e6:.2f} µs/key building batches)"
    )
    print(f"       L0 files before close: {store.l0_file_counts()}")

    if args.compact:
        compact_started = time.perf_counter()
        for db in store._dbs.values():
            db.compact_range(None, None)
        print(f"       manual compaction: {time.perf_counter() - compact_started:.1f}s")

    store.close()
    size_mb = (
        sum(f.stat().st_size for f in Path(args.dir).rglob("*") if f.is_file()) / 1e6
    )
    print(f"       on-disk size: {size_mb:.0f}MB")


def phase_measure(args: argparse.Namespace) -> None:
    store = open_store(args)
    if len(store._dbs) >= 2 and args.point_lookup:
        # Proves the block cache is real and shared across bucket DBs rather
        # than each one quietly creating its own.
        assert store.cache_usage_bytes() > 0, "block cache reports no usage"

    hits = int(args.batch * args.hit_rate)
    latencies: list[float] = []
    checked = 0
    found = 0
    for b in range(args.batches):
        keys = [make_key(i, args.key_len) for i in range(b * hits, b * hits + hits)]
        keys += [
            make_key(args.keys + b * args.batch + i, args.key_len)
            for i in range(args.batch - hits)
        ]
        started = time.perf_counter()
        results = store._batch_contains(keys)
        latencies.append(time.perf_counter() - started)
        checked += len(keys)
        found += sum(results)

    lookup_sec = sum(latencies)
    latencies.sort()

    def pct(p: float) -> float:
        return latencies[min(int(len(latencies) * p), len(latencies) - 1)] * 1000

    print(
        f"READ:  {checked} keys over {args.batches} batches of {args.batch} "
        f"at hit rate {args.hit_rate:.0%} ({found} hits)"
    )
    print(
        f"       {lookup_sec / checked * 1e6:.2f} µs/key, "
        f"{checked / lookup_sec / 1000:.0f}k keys/s"
    )
    print(
        f"       per-batch ms: p50={pct(0.5):.2f} p95={pct(0.95):.2f} "
        f"p99={pct(0.99):.2f} mean={statistics.mean(latencies) * 1000:.2f}"
    )
    print(f"       buckets={len(store._dbs)}")
    print(f"       block cache usage: {store.cache_usage_bytes() / 1e6:.1f}MB")
    print(f"       L0 files: {store.l0_file_counts()}")
    store.close()


def phase_all(args: argparse.Namespace) -> None:
    if args.fresh and Path(args.dir).exists():
        shutil.rmtree(args.dir)
    passthrough: list[str] = []
    argv = sys.argv[1:]
    i = 0
    while i < len(argv):
        if argv[i] == "--phase":
            i += 2  # drop the flag and its value
            continue
        if argv[i].startswith("--phase="):
            i += 1
            continue
        passthrough.append(argv[i])
        i += 1

    for phase in ("populate", "measure"):
        # A fresh process per phase: the measure run must open the DB cold.
        subprocess.run(
            [sys.executable, __file__, "--phase", phase, *passthrough],
            check=True,
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--phase", choices=("all", "populate", "measure"), default="all")
    p.add_argument("--dir", default="/tmp/tkati-dedup-bench")
    p.add_argument("--fresh", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument(
        "--keys", type=int, default=2_000_000, help="keys written during populate"
    )
    p.add_argument(
        "--key-len", type=int, default=36, help="key length in bytes (UUID-ish)"
    )
    p.add_argument("--batch", type=int, default=10_000)
    p.add_argument(
        "--batches", type=int, default=200, help="lookup batches during measure"
    )
    p.add_argument(
        "--hit-rate", type=float, default=0.02, help="fraction of lookups that hit"
    )
    p.add_argument(
        "--buckets", type=int, default=3, help="window_hours, i.e. buckets to probe"
    )
    p.add_argument(
        "--compact", action="store_true", help="manual compaction after populate"
    )

    p.add_argument(
        "--point-lookup",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="enable RocksDB's bloom filter via optimize_for_point_lookup",
    )
    p.add_argument("--memtable-bloom-ratio", type=float, default=0.02)
    p.add_argument("--cache-mb", type=int, default=128)
    p.add_argument("--write-buffer-mb", type=int, default=64)
    p.add_argument(
        "--disable-auto-compactions",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    p.add_argument(
        "--compression", choices=("none", "lz4", "zstd", "snappy"), default="none"
    )
    p.add_argument("--disable-wal", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--enable-statistics", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    {"all": phase_all, "populate": phase_populate, "measure": phase_measure}[
        args.phase
    ](args)


if __name__ == "__main__":
    main()
