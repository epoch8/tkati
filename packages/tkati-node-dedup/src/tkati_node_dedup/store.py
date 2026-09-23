"""Bucketed, embedded RocksDB store for windowed key deduplication.

One RocksDB database directory per wall-clock-aligned hour bucket. A key is
considered "seen" if it exists in any bucket currently inside the window.
Existence-only store: values are always empty bytes, only key presence matters.

Lookups and writes are batched against RocksDB (one call per open bucket for
reads, one WriteBatch for writes) rather than one call per key — at realistic
batch sizes and millions of runs a day, per-row FFI calls into RocksDB are the
dominant cost, and rocksdict supports genuine batching for both directions.

RocksDB is configured for this node's specific workload rather than left on its
defaults; see `_build_options`. The fact that shapes it: **lookups almost
always miss**, because duplicates are rare in practice. The store's job is
overwhelmingly to prove a key is absent — exactly what a bloom filter does,
and RocksDB ships with none enabled by default.

Every knob here was A/B measured (`benchmarks/bench_store.py`) rather than
reasoned about, because two changes that looked obviously right on paper —
a large write buffer, and skipping compaction on buckets that get deleted
within the hour — both measured *worse*. See `settings.RocksDBSettings` for
the numbers behind each default.

Failure policy throughout: any per-bucket open/lookup/write failure is caught,
logged, and treated as "not seen" / skipped — never raised. This favors
forwarding a possible duplicate over silently dropping a real event.
"""

import math
import shutil
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger
from rocksdict import (
    DBCompressionType,
    Options,
    Rdict,
    WriteBatch,
    WriteOptions,
)

from tkati_node_dedup.settings import RocksDBSettings

_BUCKET_PREFIX = "bucket-"

_COMPRESSION = {
    "none": DBCompressionType.none,
    "lz4": DBCompressionType.lz4,
    "zstd": DBCompressionType.zstd,
    "snappy": DBCompressionType.snappy,
}

# Only used when auto-compaction is disabled (a benchmarking option, not the
# default). RocksDB's L0 write-slowdown/stop triggers still apply in that mode
# and would throttle or block writes once L0 grew past them (defaults 20 and
# 36), which would make the measurement a test of the stall rather than of
# compaction.
_NO_STALL_L0_TRIGGER = 1_000_000


def _now() -> float:
    """Indirection over time.time() so tests can freeze this store's clock
    without patching the global time module (which would also freeze
    unrelated code, e.g. the Kafka consumer's poll-timeout bookkeeping)."""
    return time.time()


def _bucket_index(ts: float, bucket_seconds: int) -> int:
    return int(ts // bucket_seconds)


class BucketedDedupStore:
    def __init__(
        self,
        root_dir: str,
        window_hours: int,
        bucket_hours: int = 1,
        *,
        tuning: RocksDBSettings | None = None,
    ) -> None:
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.bucket_seconds = bucket_hours * 3600
        self.num_buckets = math.ceil(window_hours / bucket_hours)
        self.tuning = tuning if tuning is not None else RocksDBSettings()
        self._opts = self._build_options(self.tuning)
        self._write_opt = WriteOptions()
        # rocksdict's stub declares disable_wal as a method, but at runtime it
        # is a settable property (a getset_descriptor) — calling it raises
        # "TypeError: 'bool' object is not callable". Assignment is the working
        # form; the cast is only to get past the wrong stub.
        cast("Any", self._write_opt).disable_wal = self.tuning.disable_wal
        self._dbs: dict[int, Rdict] = {}
        self._recover_existing_buckets()

    @staticmethod
    def _build_options(tuning: RocksDBSettings) -> Options:
        """Build the RocksDB options shared by every bucket.

        ### Why this uses optimize_for_point_lookup and not set_bloom_filter

        The obvious way to write this — build a `BlockBasedOptions`, call
        `set_bloom_filter(bits, False)`, attach it with
        `set_block_based_table_factory` — **does not work in rocksdict
        0.3.29** (the latest release at time of writing). The filter is
        genuinely written into the SST files (the on-disk size grows by
        exactly bits x keys) but the read path never consults it:
        `rocksdb.bloom.filter.useful` stays at 0 and the data-block read count
        is byte-for-byte identical with the filter on and off. That holds for
        `raw_mode` on and off, for block-based and full filters, across format
        versions, and for `get`, batched multi-get and `key_may_exist` alike.

        `Options.optimize_for_point_lookup` reaches RocksDB's own
        `OptimizeForPointLookup` helper instead of the binding's table-factory
        path, and that one works. Measured on a 1M-key bucket, all-miss
        lookups: 1.85 -> 0.58 us/key, data-block reads 8950 -> 206,
        `bloom.filter.useful` 0 -> 19795 out of 20000.

        The helper configures, in one call: a 10-bits-per-key bloom filter, an
        LRU block cache of the requested size, a hash index within data
        blocks, and the memtable whole-key bloom. Two consequences:

        * bits-per-key is fixed at 10 (~1% false positive rate), so there is
          no knob for it. A false positive costs one wasted data-block read
          and is then resolved by the real key comparison — it can never make
          the store answer "seen" for a key that isn't there.
        * **Nothing may call `set_block_based_table_factory` afterwards.** That
          replaces the working factory with the broken one and silently undoes
          all of the above.
        """
        opts = Options(raw_mode=True)

        if tuning.point_lookup_optimized:
            opts.optimize_for_point_lookup(tuning.block_cache_mb)

        # The memtable bloom matters because the current bucket's memtable
        # holds this hour's keys; without it each miss is a full skiplist
        # descent (measured 3.23 vs 0.85 µs/key against a warm memtable).
        # `whole_key_filtering` is silently inert unless the ratio is
        # non-zero, hence both calls. No prefix_extractor is set, which is
        # what keeps this a whole-key rather than a prefix filter.
        opts.set_memtable_prefix_bloom_ratio(tuning.memtable_bloom_ratio)
        opts.set_memtable_whole_key_filtering(tuning.memtable_bloom_ratio > 0)

        opts.set_write_buffer_size(tuning.write_buffer_mb * 1024 * 1024)
        opts.set_compression_type(_COMPRESSION[tuning.compression]())

        if tuning.disable_auto_compactions:
            # Each bucket is deleted within the hour, so compacting it only
            # ever rewrites bytes that are about to be thrown away.
            opts.set_disable_auto_compactions(True)
            opts.set_level_zero_slowdown_writes_trigger(_NO_STALL_L0_TRIGGER)
            opts.set_level_zero_stop_writes_trigger(_NO_STALL_L0_TRIGGER)

        if tuning.enable_statistics:
            # Costs roughly 5-10%; for benchmarking and diagnosis only.
            opts.enable_statistics()

        return opts

    def _bucket_path(self, bucket: int) -> Path:
        return self.root / f"{_BUCKET_PREFIX}{bucket:012d}"

    def _min_live_bucket(self) -> int:
        current = _bucket_index(_now(), self.bucket_seconds)
        return current - self.num_buckets + 1

    def _open_bucket(self, path: Path) -> Rdict:
        """The single place a bucket DB is opened.

        Every tuning knob and every write option has to be applied here; having
        had two open sites made it easy to configure one and miss the other.
        """
        db = Rdict(str(path), options=self._opts)
        db.set_write_options(self._write_opt)
        return db

    def _recover_existing_buckets(self) -> None:
        """Reopen on-disk buckets still inside the window; destroy stale ones.

        Makes dedup state survive a *graceful* restart. With the WAL disabled
        (the default) a hard kill can lose whatever was still in the current
        bucket's memtable; per this module's failure policy that costs some
        forwarded duplicates, never a dropped event.
        """
        min_live = self._min_live_bucket()
        for entry in sorted(self.root.glob(f"{_BUCKET_PREFIX}*")):
            if not entry.is_dir():
                continue
            try:
                bucket = int(entry.name.removeprefix(_BUCKET_PREFIX))
            except ValueError:
                logger.warning(
                    f"Ignoring unrecognized entry in dedup store dir: {entry}"
                )
                continue
            if bucket < min_live:
                logger.info(f"Startup: removing stale dedup bucket {bucket} ({entry})")
                self._destroy_path(entry)
                continue
            # A bucket index greater than the current one is possible if the
            # clock stepped backwards (e.g. an NTP correction). It's harmless:
            # it sits inside the window and ages out normally, though
            # _ensure_current_open won't write to it until the clock catches up.
            try:
                self._dbs[bucket] = self._open_bucket(entry)
                logger.info(f"Startup: reopened dedup bucket {bucket} from {entry}")
            except Exception:
                logger.exception(
                    f"Failed to reopen dedup bucket {bucket} at {entry}; "
                    "starting empty for this bucket (may pass through some duplicates)"
                )

    def _ensure_current_open(self) -> Rdict | None:
        bucket = _bucket_index(_now(), self.bucket_seconds)
        if bucket in self._dbs:
            return self._dbs[bucket]
        try:
            db = self._open_bucket(self._bucket_path(bucket))
        except Exception:
            logger.exception(f"Failed to open current dedup bucket {bucket}")
            return None
        self._dbs[bucket] = db
        return db

    def encode_keys(self, values: pa.Array | pa.ChunkedArray) -> list[bytes | None]:
        """Vectorized byte-key encoding for a column of scalar dedup-field values.

        Cast to string via pyarrow compute (fast, vectorized — no per-row
        Python type dispatch), then to binary so that `to_pylist()` hands back
        `bytes` directly instead of `str` we'd have to `.encode()` per row.
        Nulls stay None: they're never queried or stored, always passed through.

        The cast is two-step on purpose — pyarrow has no direct int64->binary
        cast, and the dedup field is routinely an integer column.

        Float or timestamp dedup fields are discouraged: their string
        representation isn't guaranteed stable across producers.
        """
        strings = pc.cast(values, pa.string())
        return cast("list[bytes | None]", pc.cast(strings, pa.binary()).to_pylist())

    def filter_duplicates(
        self, keys: list[bytes | None]
    ) -> tuple[pa.BooleanArray, list[bytes]]:
        """
        Given per-row encoded keys (None = no key, always kept), returns
        (keep_mask, keys_to_mark_seen). keep_mask[i] corresponds to keys[i]
        and is directly usable with pyarrow.Table.filter().

        In-batch duplicates (two rows with the same key, neither yet in the
        store) are resolved locally; the remaining unique candidates are
        checked against the store in a single batched round trip per open
        bucket, not one lookup per key.
        """
        keep_mask = [False] * len(keys)
        seen_in_batch: set[bytes] = set()
        to_check: list[bytes] = []
        to_check_idx: list[int] = []

        for i, key in enumerate(keys):
            if key is None:
                keep_mask[i] = True
                continue
            if key in seen_in_batch:
                continue
            seen_in_batch.add(key)
            to_check.append(key)
            to_check_idx.append(i)

        already_seen = self._batch_contains(to_check)

        new_keys: list[bytes] = []
        for idx, key, seen in zip(to_check_idx, to_check, already_seen, strict=True):
            if not seen:
                keep_mask[idx] = True
                new_keys.append(key)

        return pa.array(keep_mask, type=pa.bool_()), new_keys

    def _batch_contains(self, keys: list[bytes]) -> list[bool]:
        if not keys:
            return []
        found = [False] * len(keys)
        for bucket, db in list(self._dbs.items()):
            try:
                # rocksdict's stub declares `List[...]` invariantly, so
                # list[bytes] isn't accepted as-is, and doesn't distinguish
                # the list-in/list-out overload from the scalar one for the
                # return type either — get() genuinely returns a list here
                # since `keys` is a list.
                keys_arg = cast("list[str | int | float | bytes]", keys)
                results = cast("list[bytes | None]", db.get(keys_arg))
            except Exception:
                logger.exception(
                    f"Batch lookup failed against dedup bucket {bucket}; skipping it"
                )
                continue
            for i, r in enumerate(results):
                if r is not None:
                    found[i] = True

        return found

    def add_many(self, keys: Iterable[bytes]) -> None:
        keys = list(keys)
        if not keys:
            return
        db = self._ensure_current_open()
        if db is None:
            return
        try:
            wb = WriteBatch(raw_mode=True)
            for key in keys:
                wb.put(key, b"")
            db.write(wb, self._write_opt)
        except Exception:
            logger.exception("Failed to batch-write keys into current dedup bucket")

    def contains(self, key: bytes) -> bool:
        return self._batch_contains([key])[0]

    def add(self, key: bytes) -> None:
        self.add_many([key])

    def _property(self, name: str) -> dict[int, int]:
        values: dict[int, int] = {}
        for bucket, db in list(self._dbs.items()):
            try:
                value = db.property_int_value(name)
            except Exception:
                logger.exception(f"Failed to read {name} for dedup bucket {bucket}")
                continue
            if value is not None:
                values[bucket] = value
        return values

    def cache_usage_bytes(self) -> int:
        """Block cache usage. Read as a DB property rather than off a Cache
        handle, because optimize_for_point_lookup creates the cache itself and
        never hands it back. All buckets share it (the table factory's cache is
        refcounted and the Options object is cloned per bucket), so any one
        bucket's reading is the total — hence max(), not sum()."""
        usage = self._property("rocksdb.block-cache-usage")
        return max(usage.values(), default=0)

    def l0_file_counts(self) -> dict[int, int]:
        """Per-bucket L0 file count — the canary for auto-compaction being
        disabled. If this climbs without bound the write buffer is too small
        for the ingest rate and lookups are paying for it."""
        return self._property("rocksdb.num-files-at-level0")

    def _destroy_path(self, path: Path) -> None:
        try:
            Rdict.destroy(str(path), self._opts)
        except Exception:
            logger.exception(f"Rdict.destroy failed for {path}; falling back to rmtree")
            shutil.rmtree(path, ignore_errors=True)

    def cleanup_expired(self) -> None:
        """Close+delete on-disk buckets that fell out of the window.

        Called at the start of every iteration (before the dedupe check runs
        against possibly-stale buckets), and cheap to call every time: only
        does real I/O once per hour rollover, since _min_live_bucket() only
        changes then.
        """
        min_live = self._min_live_bucket()
        for bucket in list(self._dbs.keys()):
            if bucket < min_live:
                db = self._dbs.pop(bucket)
                try:
                    db.close()
                except Exception:
                    logger.exception(f"Error closing expired dedup bucket {bucket}")
                self._destroy_path(self._bucket_path(bucket))
                logger.info(f"Expired dedup bucket {bucket} removed")

        # Buckets we failed to open at startup never made it into _dbs, so the
        # loop above can't reach them and they'd sit on disk until the next
        # restart. Sweep the filesystem too.
        for entry in sorted(self.root.glob(f"{_BUCKET_PREFIX}*")):
            if not entry.is_dir():
                continue
            try:
                bucket = int(entry.name.removeprefix(_BUCKET_PREFIX))
            except ValueError:
                continue
            if bucket < min_live and bucket not in self._dbs:
                logger.info(f"Removing orphaned stale dedup bucket {bucket} ({entry})")
                self._destroy_path(entry)

    def close(self) -> None:
        for bucket, db in self._dbs.items():
            try:
                # With the WAL disabled, the memtable is the only copy of this
                # hour's keys until it's flushed. rocksdict documents close()
                # as near-equivalent to flush()+drop, and RocksDB flushes on
                # shutdown by default — flushing explicitly makes the graceful
                # restart path depend on something visible rather than on two
                # layers of implicit defaults.
                db.flush()
                db.close()
            except Exception:
                logger.exception(f"Error closing dedup bucket {bucket} on shutdown")
        self._dbs.clear()
