from typing import cast

import pyarrow as pa
from rocksdict import Rdict
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
