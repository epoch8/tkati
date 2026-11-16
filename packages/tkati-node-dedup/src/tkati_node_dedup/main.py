"""Deduplicate a Kafka topic by one field, within a rolling window.

The node runs on `PipelinedNode`: `done()` sends a batch's survivors and
returns while they are still in flight, and the batch is committed later,
once delivered. For each batch:

1. look the keys up, in the store and among the keys still *pending* (sent
   in a batch not yet committed), and filter out duplicates;
2. add the survivors' keys to the pending set, and `done()` with the
   survivors;
3. once the batch is committed (its `after_commit`), mark its keys seen in
   the store and drop them from the pending set.

Checking the pending set is what keeps cross-batch dedup exact while batches
are in flight: a key sent in batch N is dropped from batch N+1 even before N
is delivered.

No event can be lost, because a key reaches the store only after its row is
delivered and its batch committed. Marking keys seen before delivery is the
one dangerous order: a crash in between would drop the event on re-read
without it ever having been produced. The crash cases are all duplicates at
worst:

- before a batch is committed: the pending set is lost along with the
  uncommitted offsets, so the batch is re-read, finds its keys unseen, and is
  sent again;
- between the commit and the store write: the batch isn't re-read and its
  keys were never stored, so a later duplicate of one of them is forwarded,
  in a window as wide as one memtable write.
"""

from collections.abc import Sequence
from contextlib import closing
from functools import partial
from math import isfinite
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger
from prometheus_client import Counter
from tkati_core import PRODUCER_PHASES, Batch, PipelinedNode

from tkati_node_dedup.settings import AppSettings
from tkati_node_dedup.store import BucketedDedupStore

# The perf report's loop line, in the order the phases happen, not sorted by
# duration: a stable field order is what makes two consecutive log lines
# comparable at a glance. The unprefixed names live here rather than in
# tkati-core because they are this node's pipeline — tkati-node-el, for
# instance, has no lookup or write phase. The producer's phases are spliced in
# from tkati-core, which owns the names it times itself against. `write` comes
# last: it runs in `after_commit`, after the commit. The consumer's phases go
# on the report's read line, which tkati-core adds itself.
_PHASES = (
    "wait/input",
    "lookup",
    *PRODUCER_PHASES,
    "wait/in-flight",
    "commit",
    "write",
)


# Exposed as tkati_node_dedup_dropped_rows_total. The perf log line's "dropped",
# as a counter of its own rather than left to rows_in - rows_out in PromQL.
_DROPPED_ROWS = Counter(
    "tkati_node_dedup_dropped_rows",
    "Rows dropped as duplicates, seen earlier in the batch or in the dedup window.",
)


_DROPPED_FIELDS = Counter(
    "tkati_node_dedup_dropped_field",
    "Sum of nonnegative numeric field values on rows dropped as duplicates.",
    ["field"],
)


def _sum_counter_column(column: pa.ChunkedArray) -> float:
    """Invalid values contribute zero; never decrease or poison a counter."""
    if not (
        pa.types.is_integer(column.type)
        or pa.types.is_floating(column.type)
        or pa.types.is_decimal(column.type)
    ):
        return 0.0

    values = column.cast(pa.float64(), safe=False)

    compute = cast("Any", pc)
    valid = compute.and_(compute.is_finite(values), compute.greater_equal(values, 0))
    total = compute.sum(compute.if_else(valid, values, 0.0)).as_py() or 0.0
    return total if isfinite(total) else 0.0


def _dedupe_batch(
    batch: pa.Table,
    field_name: str,
    store: BucketedDedupStore,
    pending: set[bytes],
    counter_fields: Sequence[str] = (),
) -> tuple[pa.Table, list[bytes], dict[str, float]]:
    """Filter out rows whose dedup key was already seen (in-batch or in the store).

    Returns (filtered_batch, keys_to_mark_seen, dropped_field_sums). Null values
    in `field_name` always pass through and are never added to the store — we
    can't dedup on nothing.

    Encoding and the store lookup are both batched (one pass over the column,
    one RocksDB round trip per open bucket) rather than done per row.
    """
    keys = store.encode_keys(batch.column(field_name))
    keep_mask, new_keys = store.filter_duplicates(keys, pending)
    filtered = batch.filter(keep_mask)
    sums: dict[str, float] = {}
    if counter_fields and len(filtered) < len(batch):
        # `invert` is another dynamically published Arrow kernel.
        drop_mask = cast("Any", pc).invert(keep_mask)
        for field in counter_fields:
            if field in batch.column_names:
                sums[field] = _sum_counter_column(batch.column(field).filter(drop_mask))
    return filtered, new_keys, sums


def _mark_seen(
    node: PipelinedNode,
    store: BucketedDedupStore,
    pending: set[bytes],
    keys: list[bytes],
    dropped: int,
    field_sums: dict[str, float],
) -> None:
    """A batch's `after_commit`: its keys go from pending to the store, and
    its drops are counted. Counted only once committed: a batch that fails
    before then is re-read after restart, and counting its drops too would
    count them twice."""
    with node.phase("write"):
        store.add_many(keys)
    pending.difference_update(keys)
    _DROPPED_ROWS.inc(dropped)
    for field, total in field_sums.items():
        _DROPPED_FIELDS.labels(field=field).inc(total)


def run(
    node: PipelinedNode,
    store: BucketedDedupStore,
    field_name: str,
    counter_fields: Sequence[str] = (),
) -> None:
    counter_fields = tuple(dict.fromkeys(counter_fields))
    for field in counter_fields:
        # Expose zero even if the field is missing or no duplicates arrive.
        _DROPPED_FIELDS.labels(field=field)
    # Keys sent in batches not yet committed. See the module docstring.
    pending: set[bytes] = set()
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
            filtered, new_keys, field_sums = table, [], {}
        else:
            # Includes the table.filter() call, which is Arrow work rather than
            # a store lookup — cheap enough not to be worth a phase of its own.
            with node.phase("lookup"):
                filtered, new_keys, field_sums = _dedupe_batch(
                    table, field_name, store, pending, counter_fields
                )

        dropped = len(table) - len(filtered)
        # Before done(): it may commit this very batch (and so run its
        # after_commit) before returning, if the delivery is already in.
        pending.update(new_keys)
        node.done(
            event,
            output_arrow=filtered,
            after_commit=partial(
                _mark_seen, node, store, pending, new_keys, dropped, field_sums
            ),
        )

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
    with closing(store), PipelinedNode.from_settings(settings, phases=_PHASES) as node:
        run(node, store, settings.dedup.field, settings.dedup.counter_fields)
