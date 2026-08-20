"""Bucketed, embedded RocksDB store for windowed key deduplication.

One RocksDB database directory per wall-clock-aligned hour bucket. A key is
considered "seen" if it exists in any bucket currently inside the window.
Existence-only store: values are always empty bytes, only key presence matters.

Lookups and writes are batched against RocksDB (one call per open bucket for
reads, one WriteBatch for writes) rather than one call per key — at realistic
batch sizes and millions of runs a day, per-row FFI calls into RocksDB are the
dominant cost, and rocksdict supports genuine batching for both directions.

Failure policy throughout: any per-bucket open/lookup/write failure is caught,
logged, and treated as "not seen" / skipped — never raised. This favors
forwarding a possible duplicate over silently dropping a real event.
"""

import math
import shutil
import time
from collections.abc import Iterable
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger
from rocksdict import Options, Rdict, WriteBatch

_BUCKET_PREFIX = "bucket-"


def _now() -> float:
    """Indirection over time.time() so tests can freeze this store's clock
    without patching the global time module (which would also freeze
    unrelated code, e.g. the Kafka consumer's poll-timeout bookkeeping)."""
    return time.time()


def _bucket_index(ts: float, bucket_seconds: int) -> int:
    return int(ts // bucket_seconds)


class BucketedDedupStore:
    def __init__(self, root_dir: str, window_hours: int, bucket_hours: int = 1) -> None:
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.bucket_seconds = bucket_hours * 3600
        self.num_buckets = math.ceil(window_hours / bucket_hours)
        self._opts = Options(raw_mode=True)
        self._dbs: dict[int, Rdict] = {}
        self._recover_existing_buckets()

    def _bucket_path(self, bucket: int) -> Path:
        return self.root / f"{_BUCKET_PREFIX}{bucket:012d}"

    def _min_live_bucket(self) -> int:
        current = _bucket_index(_now(), self.bucket_seconds)
        return current - self.num_buckets + 1

    def _recover_existing_buckets(self) -> None:
        """Reopen on-disk buckets still inside the window; destroy stale ones.

        Makes dedup state survive a node restart.
        """
        min_live = self._min_live_bucket()
        for entry in sorted(self.root.glob(f"{_BUCKET_PREFIX}*")):
            if not entry.is_dir():
                continue
            try:
                bucket = int(entry.name.removeprefix(_BUCKET_PREFIX))
            except ValueError:
                logger.warning(f"Ignoring unrecognized entry in dedup store dir: {entry}")
                continue
            if bucket < min_live:
                logger.info(f"Startup: removing stale dedup bucket {bucket} ({entry})")
                self._destroy_path(entry)
                continue
            try:
                self._dbs[bucket] = Rdict(str(entry), options=self._opts)
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
            db = Rdict(str(self._bucket_path(bucket)), options=self._opts)
        except Exception:
            logger.exception(f"Failed to open current dedup bucket {bucket}")
            return None
        self._dbs[bucket] = db
        return db

    def encode_keys(self, values: pa.Array | pa.ChunkedArray) -> list[bytes | None]:
        """Vectorized byte-key encoding for a column of scalar dedup-field values.

        Cast to string via pyarrow compute (fast, vectorized — no per-row
        Python type dispatch), then UTF-8 encode each non-null value. Nulls
        stay None: they're never queried or stored, always passed through.

        Float or timestamp dedup fields are discouraged: their string
        representation isn't guaranteed stable across producers.
        """
        strings = pc.cast(values, pa.string())
        return [None if s is None else s.encode("utf-8") for s in strings.to_pylist()]

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
                logger.exception(f"Batch lookup failed against dedup bucket {bucket}; skipping it")
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
            db.write(wb)
        except Exception:
            logger.exception("Failed to batch-write keys into current dedup bucket")

    def contains(self, key: bytes) -> bool:
        return self._batch_contains([key])[0]

    def add(self, key: bytes) -> None:
        self.add_many([key])

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

    def close(self) -> None:
        for bucket, db in self._dbs.items():
            try:
                db.close()
            except Exception:
                logger.exception(f"Error closing dedup bucket {bucket} on shutdown")
        self._dbs.clear()
