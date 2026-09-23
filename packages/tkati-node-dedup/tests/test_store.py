import subprocess
import sys
import textwrap
from typing import cast

import pyarrow as pa
import pytest
from rocksdict import Rdict
from tkati_node_dedup.settings import RocksDBSettings
from tkati_node_dedup.store import BucketedDedupStore


def _key(value: str) -> bytes:
    return value.encode("utf-8")


def test_add_and_contains(tmp_path) -> None:
    store = BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)
    key = _key("abc123")
    assert store.contains(key) is False
    store.add(key)
    assert store.contains(key) is True
    assert store.contains(_key("other")) is False
    store.close()


def test_bucket_rollover_expiry(tmp_path, monkeypatch) -> None:
    now = [1_000_000.0]
    monkeypatch.setattr("tkati_node_dedup.store._now", lambda: now[0])

    store = BucketedDedupStore(str(tmp_path), window_hours=2, bucket_hours=1)
    key = _key("abc123")
    store.add(key)
    assert store.contains(key) is True

    # Advance past window_hours + bucket_hours so the bucket fully ages out.
    now[0] += 4 * 3600
    store.cleanup_expired()

    assert store.contains(key) is False
    remaining = list(tmp_path.glob("bucket-*"))
    assert remaining == []
    store.close()


def test_restart_resumes_existing_buckets(tmp_path) -> None:
    """Also the regression test for the disabled WAL: with `disable_wal` on
    (the default), the memtable is the only copy of a freshly-written key, so
    this only passes because closing the DB flushes it. See
    test_graceful_close_persists_even_without_our_explicit_flush for which
    flush is actually load-bearing, and
    test_hard_kill_loses_unflushed_keys_with_the_wal_off for the other side."""
    store = BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)
    key = _key("abc123")
    store.add(key)
    store.close()

    store2 = BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)
    assert store2.contains(key) is True
    store2.close()


def test_restart_discards_stale_on_disk_buckets(tmp_path, monkeypatch) -> None:
    now = [1_000_000.0]
    monkeypatch.setattr("tkati_node_dedup.store._now", lambda: now[0])

    store = BucketedDedupStore(str(tmp_path), window_hours=1, bucket_hours=1)
    key = _key("abc123")
    store.add(key)
    store.close()

    now[0] += 10 * 3600  # far outside the window

    store2 = BucketedDedupStore(str(tmp_path), window_hours=1, bucket_hours=1)
    assert store2.contains(key) is False
    assert list(tmp_path.glob("bucket-*")) == []
    store2.close()


def test_lookup_failure_is_treated_as_not_seen(tmp_path) -> None:
    store = BucketedDedupStore(str(tmp_path), window_hours=1, bucket_hours=1)
    key = _key("abc123")

    class BrokenDict:
        def get(self, _keys: list[bytes]) -> list[bytes | None]:
            raise RuntimeError("boom")

        def close(self) -> None:
            pass

    store._dbs[0] = cast(Rdict, BrokenDict())

    assert store.contains(key) is False  # must not raise
    store.close()


def test_encode_keys_vectorized_cast_and_nulls(tmp_path) -> None:
    store = BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)

    strings = store.encode_keys(pa.array(["abc", None, "def"], type=pa.string()))
    assert strings == [b"abc", None, b"def"]

    ints = store.encode_keys(pa.array([1, None, 42], type=pa.int64()))
    assert ints == [b"1", None, b"42"]

    store.close()


def test_filter_duplicates_in_batch_and_store_duplicates(tmp_path) -> None:
    store = BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)
    store.add(_key("already-seen"))

    keys = [
        _key("already-seen"),  # duplicate of a key already in the store
        _key("fresh"),  # new, kept
        _key("fresh"),  # duplicate of the row above, within this same batch
        None,  # no key, always kept, never stored
    ]

    keep_mask, new_keys = store.filter_duplicates(keys)

    assert keep_mask.to_pylist() == [False, True, False, True]
    assert new_keys == [_key("fresh")]

    store.close()


def test_one_failing_bucket_among_healthy_ones_degrades_to_not_seen(tmp_path) -> None:
    """A single sick bucket must not hide a hit in a healthy one, and must not
    raise. The older single-bucket version of this test could pass even if a
    failure aborted the whole lookup."""
    store = BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)
    key = _key("in-a-good-bucket")
    store.add(key)
    healthy_bucket = next(iter(store._dbs))

    class BrokenDict:
        def get(self, _keys: list[bytes]) -> list[bytes | None]:
            raise RuntimeError("boom")

        def close(self) -> None:
            pass

    # Two more buckets inside the window, one of them broken.
    store._dbs[healthy_bucket - 1] = cast(Rdict, BrokenDict())

    assert store.contains(key) is True  # the healthy bucket still answers
    assert store.contains(_key("nowhere")) is False

    store.close()


def test_graceful_close_persists_even_without_our_explicit_flush(
    tmp_path, monkeypatch
) -> None:
    """The store calls db.flush() before db.close(), but that call is
    insurance, not the mechanism: RocksDB flushes memtables on shutdown by
    default (avoid_flush_during_shutdown=false). Neutering our flush must
    therefore still round-trip — if this ever fails, the explicit flush has
    become load-bearing and the durability comments need revisiting."""
    store = BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)
    key = _key("only-in-the-memtable")
    store.add(key)

    monkeypatch.setattr(Rdict, "flush", lambda *args, **kwargs: None)
    store.close()

    store2 = BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)
    assert store2.contains(key) is True
    store2.close()


def test_hard_kill_loses_unflushed_keys_with_the_wal_off(tmp_path) -> None:
    """The documented durability boundary. With disable_wal the memtable is
    the only copy of this hour's keys until it's flushed, so a process that
    dies without closing loses them — which costs forwarded duplicates, never
    dropped events. This is what README's 'not crash-durable by design' means;
    with the WAL on, the same kill would recover the key."""
    script = textwrap.dedent(
        """
        import os, sys
        from tkati_node_dedup.settings import RocksDBSettings
        from tkati_node_dedup.store import BucketedDedupStore

        store = BucketedDedupStore(
            sys.argv[1], window_hours=3, bucket_hours=1,
            tuning=RocksDBSettings(disable_wal=sys.argv[2] == "off"),
        )
        store.add(b"written-then-killed")
        os._exit(0)  # no close(), no flush, no atexit — a hard kill
        """
    )
    for wal, expected in (("off", False), ("on", True)):
        root = tmp_path / wal
        subprocess.run([sys.executable, "-c", script, str(root), wal], check=True)
        store = BucketedDedupStore(str(root), window_hours=3, bucket_hours=1)
        assert store.contains(b"written-then-killed") is expected, (
            f"WAL {wal}: expected contains() == {expected}"
        )
        store.close()


def test_many_writes_without_compaction_still_read_back(tmp_path) -> None:
    """Guards the L0 stall-trigger trap: with auto-compaction disabled,
    RocksDB's default level-zero slowdown/stop triggers would throttle or
    block writes once L0 grew past them. A tiny write buffer forces many
    flushes, so L0 climbs well past the default triggers of 20/36."""
    store = BucketedDedupStore(
        str(tmp_path),
        window_hours=3,
        bucket_hours=1,
        tuning=RocksDBSettings(write_buffer_mb=1, disable_auto_compactions=True),
    )
    keys = [_key(f"key-{i:07d}") for i in range(60_000)]
    for start in range(0, len(keys), 1000):
        store.add_many(keys[start : start + 1000])

    bucket = next(iter(store._dbs))
    assert (
        store._dbs[bucket].property_int_value("rocksdb.num-files-at-level0") is not None
    )

    found = store._batch_contains(keys)
    assert all(found)
    assert store.contains(_key("never-written")) is False
    store.close()


def test_binary_keys_with_null_bytes_and_long_keys(tmp_path) -> None:
    """raw_mode accepts arbitrary bytes; nothing in the options path may
    assume printable, NUL-free or prefix-structured keys."""
    store = BucketedDedupStore(str(tmp_path), window_hours=3, bucket_hours=1)
    keys = [b"\x00\x01\x02", b"a\x00b", b"\xff" * 4096, b"\x00", bytes(range(256))]
    store.add_many(keys)

    assert all(store._batch_contains(keys))
    assert store.contains(b"\x00\x01\x03") is False
    store.close()


@pytest.mark.parametrize(
    "tuning",
    [
        pytest.param(RocksDBSettings(), id="defaults"),
        pytest.param(RocksDBSettings(point_lookup_optimized=False), id="no-bloom"),
        pytest.param(RocksDBSettings(memtable_bloom_ratio=0.0), id="no-memtable-bloom"),
        pytest.param(RocksDBSettings(block_cache_mb=1), id="tiny-cache"),
        pytest.param(
            RocksDBSettings(disable_auto_compactions=True, write_buffer_mb=1),
            id="no-compaction",
        ),
        pytest.param(RocksDBSettings(compression="lz4"), id="lz4"),
        pytest.param(RocksDBSettings(compression="snappy"), id="snappy"),
        pytest.param(RocksDBSettings(disable_wal=False), id="wal-on"),
        pytest.param(RocksDBSettings(enable_statistics=True), id="statistics"),
    ],
)
def test_tuning_variants_do_not_change_semantics(tmp_path, tuning) -> None:
    """Every knob is a performance knob. None of them may change an answer."""
    store = BucketedDedupStore(
        str(tmp_path), window_hours=3, bucket_hours=1, tuning=tuning
    )
    store.add(_key("already-seen"))

    keys = [_key("already-seen"), _key("fresh"), _key("fresh"), None]
    keep_mask, new_keys = store.filter_duplicates(keys)

    assert keep_mask.to_pylist() == [False, True, False, True]
    assert new_keys == [_key("fresh")]

    store.add_many(new_keys)
    assert store.contains(_key("fresh")) is True
    assert store.contains(_key("never-seen")) is False
    store.close()


def test_orphaned_stale_bucket_is_cleaned_up(tmp_path, monkeypatch) -> None:
    """A bucket that failed to open at startup never lands in _dbs, so the
    in-memory sweep can't reach it. It must still be removed from disk."""
    now = [1_000_000.0]
    monkeypatch.setattr("tkati_node_dedup.store._now", lambda: now[0])

    store = BucketedDedupStore(str(tmp_path), window_hours=1, bucket_hours=1)
    # A stale bucket directory nobody ever opened.
    orphan = tmp_path / "bucket-000000000001"
    orphan.mkdir()
    (orphan / "CURRENT").write_text("nonsense")

    now[0] += 10 * 3600
    store.cleanup_expired()

    assert not orphan.exists()
    store.close()
