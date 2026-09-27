"""Deduplicate a Kafka topic by one field, within a rolling window.

Ordering is what makes this at-least-once without losing events. For each
batch, in the loop body:

1. look the keys up and filter out duplicates;
2. `done()` with the survivors: sent, delivered, then committed;
3. only then mark their keys seen in the store.

A crash before the commit re-reads the batch at restart, and its rows are
sent again: a duplicate at worst. A crash between the commit and step 3 means
the batch isn't re-read and its keys were never marked, so a later duplicate
of one of them is forwarded: again a duplicate at worst, in a window as wide
as one memtable write. Either way no event is lost. Marking keys seen before
delivery is the one dangerous order: a crash in between would drop the event
on re-read without it ever having been produced.
"""

from contextlib import closing

import pyarrow as pa
from loguru import logger
from prometheus_client import Counter
from tkati_core import CONSUMER_PHASES, PRODUCER_PHASES, Batch, SyncNode

from tkati_node_dedup.settings import AppSettings
from tkati_node_dedup.store import BucketedDedupStore

# Reported in this order, not sorted by duration: a stable field order is what
# makes two consecutive log lines comparable at a glance. The unprefixed names
# live here rather than in tkati-core because they are this node's pipeline —
# tkati-node-el, for instance, has no lookup or write phase. The consumer's and
# producer's phases are spliced in from tkati-core, which owns the names it
# times itself against.
_PHASES = (*CONSUMER_PHASES, "lookup", *PRODUCER_PHASES, "write", "commit")


# Exposed as tkati_node_dedup_dropped_rows_total. The perf log line's "dropped",
# as a counter of its own rather than left to rows_in - rows_out in PromQL.
_DROPPED_ROWS = Counter(
    "tkati_node_dedup_dropped_rows",
    "Rows dropped as duplicates, seen earlier in the batch or in the dedup window.",
)


def _dedupe_batch(
    batch: pa.Table, field_name: str, store: BucketedDedupStore
) -> tuple[pa.Table, list[bytes]]:
    """Filter out rows whose dedup key was already seen (in-batch or in the store).

    Returns (filtered_batch, keys_to_mark_seen). Null values in `field_name`
    always pass through and are never added to the store — we can't dedup on
    nothing.

    Encoding and the store lookup are both batched (one pass over the column,
    one RocksDB round trip per open bucket) rather than done per row.
    """
    keys = store.encode_keys(batch.column(field_name))
    keep_mask, new_keys = store.filter_duplicates(keys)
    filtered = batch.filter(keep_mask)
    return filtered, new_keys


def run(node: SyncNode, store: BucketedDedupStore, field_name: str) -> None:
    for event in node.consume_arrow():
        # Runs on every event, a batch or an empty poll, and can never raise.
        # Buckets must be fresh *before* the dedupe check below runs — doing
        # this only after commit would leave a just-expired bucket open and
        # checked against for one extra batch, and an idle node (only Idle
        # events) would never clean up at all.
        # Timed into the "commit" bucket rather than given a phase of its own:
        # it is ~0 except once an hour when a bucket is destroyed, so it shows
        # up as an occasional commit spike instead of a permanent near-zero
        # field.
        with node.phase("commit"):
            try:
                store.cleanup_expired()
            except Exception:
                logger.exception(
                    "dedup store cleanup failed; will retry next iteration"
                )

        if not isinstance(event, Batch):
            continue
        table = event.data

        if field_name not in table.column_names:
            logger.warning(
                f"Dedup field '{field_name}' missing from batch schema; "
                "passing batch through unfiltered"
            )
            filtered, new_keys = table, []
        else:
            # Includes the table.filter() call, which is Arrow work rather than
            # a store lookup — cheap enough not to be worth a phase of its own.
            with node.phase("lookup"):
                filtered, new_keys = _dedupe_batch(table, field_name, store)

        # Sent, delivered, then committed. Everything below runs after the
        # commit.
        node.done(event, output_arrow=filtered)

        # Only after a confirmed delivery: mark these keys seen. Marking a key
        # seen before its row is delivered would risk losing the event on a
        # crash; see the module docstring for why after the commit is safe.
        with node.phase("write"):
            store.add_many(new_keys)

        dropped = len(table) - len(filtered)
        # Counted only once committed: a batch that fails before then is
        # re-read after restart, and counting its drops too would count them
        # twice.
        _DROPPED_ROWS.inc(dropped)

        logger.debug(
            f"Batch of {len(table)} rows: produced {len(filtered)}, "
            f"deduped {dropped} ({len(new_keys)} newly marked seen)"
        )


def main() -> None:
    settings = AppSettings()

    store = BucketedDedupStore(
        root_dir=settings.dedup.store_dir,
        window_hours=settings.dedup.window_hours,
        bucket_hours=settings.dedup.bucket_hours,
        tuning=settings.dedup.rocksdb,
    )
    # The store is entered first, so it closes last: after the node has
    # stopped and closed its clients.
    with closing(store), SyncNode.from_settings(settings, phases=_PHASES) as node:
        run(node, store, settings.dedup.field)
