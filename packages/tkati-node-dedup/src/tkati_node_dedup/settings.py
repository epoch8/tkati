from typing import Literal

from pydantic import BaseModel, field_validator
from tkati_core.settings import InputSettings, OutputSettings, TomlBaseSettings


class RocksDBSettings(BaseModel):
    """Tuning knobs for the embedded dedup store.

    The defaults here are chosen for this node's actual workload — a store
    whose lookups almost always miss (duplicates are rare) and whose buckets
    are written for one hour and then deleted — and they differ substantially
    from RocksDB's own defaults. See `store.py` for the reasoning behind each.
    """

    # Enables RocksDB's bloom filter (10 bits/key) plus a block cache of
    # `block_cache_mb` and an in-data-block hash index. There is no
    # bits-per-key knob: the only bloom-filter API that actually works in
    # rocksdict is the all-in-one `optimize_for_point_lookup` helper, which
    # hardcodes 10. See BucketedDedupStore._build_options for the evidence.
    # Off is the pre-tuning behavior, kept expressible for A/B benchmarking.
    point_lookup_optimized: bool = True

    # Bloom filter for the memtable, which on the current bucket holds this
    # hour's keys. Measured against a warm memtable, 0 -> 0.02 takes an
    # all-miss lookup from 3.23 to 0.85 µs/key at no write cost. Higher
    # ratios are worse, not better (0.05 -> 1.22, 0.10 -> 1.51): a larger
    # bloom probes with worse cache locality. 0.02 is also what
    # optimize_for_point_lookup picks. 0 disables.
    memtable_bloom_ratio: float = 0.02

    block_cache_mb: int = 128

    # 64MB, not larger. A 256MB buffer measured 25% worse on writes (deeper
    # skiplist, larger memtable bloom to populate) and 20% worse on reads.
    write_buffer_mb: int = 64

    # Leave compaction ON. Each bucket is deleted within the hour, so skipping
    # compaction looks free — but measured, it bought nothing on writes (2.41
    # vs 2.43 µs/key; compaction runs on background threads and never
    # contended with the write path) while costing 2.2x on reads (1.37 -> 2.96
    # µs/key) as L0 files accumulated. Kept as a knob only for benchmarking.
    disable_auto_compactions: bool = False

    # Snappy is RocksDB's default and costs 2x on reads here (2.84 vs 1.37
    # µs/key). Dedup keys are high-entropy and barely compress, and the store
    # is ephemeral, so paying for compression buys little disk and costs real
    # CPU.
    compression: Literal["none", "lz4", "zstd", "snappy"] = "none"

    disable_wal: bool = True
    enable_statistics: bool = False

    @field_validator("block_cache_mb", "write_buffer_mb")
    @classmethod
    def _positive_mb(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("must be a positive number of megabytes")
        return v

    @field_validator("memtable_bloom_ratio")
    @classmethod
    def _ratio(cls, v: float) -> float:
        # RocksDB caps this at 0.25 internally; 0 disables the memtable bloom.
        if not 0.0 <= v < 1.0:
            raise ValueError("must be in [0.0, 1.0)")
        return v


class DedupSettings(BaseModel):
    field: str
    window_hours: int = 3
    bucket_hours: int = 1
    store_dir: str = "./dedup_store"
    rocksdb: RocksDBSettings = RocksDBSettings()

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
