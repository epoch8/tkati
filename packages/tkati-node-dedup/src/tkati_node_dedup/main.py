import pyarrow as pa
from loguru import logger
from prometheus_client import Counter
from tkati_core import (
    CONSUMER_PHASES,
    PRODUCER_PHASES,
    Consumer,
    LoopStats,
    Producer,
    build_consumer,
    build_producer,
    start_metrics_server,
)

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


def _new_stats() -> LoopStats:
    return LoopStats(phases=_PHASES)


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


def run_one_iteration(
    consumer: Consumer,
    producer: Producer,
    store: BucketedDedupStore,
    settings: AppSettings,
    stats: LoopStats | None = None,
) -> None:
    stats = stats if stats is not None else _new_stats()

    # Runs first, every iteration (even if no batch arrives), and can never
    # raise. Buckets must be fresh *before* the dedupe check below runs —
    # doing this only after commit would leave a just-expired bucket open and
    # checked against for one extra iteration, and an idle node (no messages,
    # read_arrow returns None below) would never clean up at all.
    # Timed into the "commit" bucket rather than given a phase of its own:
    # it is ~0 except once an hour when a bucket is destroyed, so it shows
    # up as an occasional commit spike instead of a permanent near-zero field.
    with stats.phase("commit"):
        try:
            store.cleanup_expired()
        except Exception:
            logger.exception("dedup store cleanup failed; will retry next iteration")

    # No phase block here: the consumer splits its own time into `poll` and
    # `parse`. Wrapping it in an umbrella phase as well would double-count that
    # time, and the percentages are of the interval — they are meant to fall
    # short of 100%, with the shortfall being genuinely unaccounted work.
    batch = consumer.read_arrow(
        num_messages=settings.input.consumer.batch_size,
        timeout=settings.input.consumer.batch_timeout_sec,
        stats=stats,
    )
    stats.iterations += 1
    if batch is None:
        stats.starved_iterations += 1
        return

    table = batch.data

    # A short batch means the node drained the topic and waited out the batch
    # timeout — it wasn't CPU-bound, so its timings say nothing about whether
    # this node can keep up.
    if len(table) < settings.input.consumer.batch_size:
        stats.starved_iterations += 1
    stats.rows_in += len(table)

    try:
        field_name = settings.dedup.field
        if field_name not in table.column_names:
            logger.warning(
                f"Dedup field '{field_name}' missing from batch schema; "
                "passing batch through unfiltered"
            )
            filtered, new_keys = table, []
        else:
            # Includes the table.filter() call, which is Arrow work rather than
            # a store lookup — cheap enough not to be worth a phase of its own.
            with stats.phase("lookup"):
                filtered, new_keys = _dedupe_batch(table, field_name, store)

        if len(filtered) > 0:
            # No phase blocks here either: the producer splits its own time
            # into `serialize`, `enqueue` and `deliver`, for the same reason as
            # the consumer above.
            producer.produce_arrow(filtered, stats=stats)
            # Block until actually delivered before marking anything "seen" or
            # committing: KafkaProducer.produce_arrow() only enqueues
            # (non-blocking), and marking a key seen before it's durably
            # delivered would risk losing the event permanently on a crash.
            # ClickhouseProducer.flush() is a no-op since its inserts are
            # already synchronous.
            producer.flush(stats=stats)

        # Only after a confirmed-successful produce: mark these keys seen.
        with stats.phase("write"):
            store.add_many(new_keys)
    except Exception:
        # The batch failed: say so, so the consumer reads it again. The error
        # still propagates and stops the node, as before.
        consumer.rewind(batch)
        raise

    dropped = len(table) - len(filtered)
    stats.rows_out += len(filtered)

    # Only after mark-seen: commit the batch as read, not the filtered table.
    # If we crash before this line, the batch is re-read at restart; those keys
    # are already in the store, so re-processing it drops what was already
    # produced — a harmless duplicate at worst, never a lost event.
    with stats.phase("commit"):
        consumer.commit(batch)

    # Counted only once committed: a batch that fails before this point is
    # re-read after restart, and counting its drops here too would count them
    # twice.
    _DROPPED_ROWS.inc(dropped)

    logger.debug(
        f"Batch of {len(table)} rows: produced {len(filtered)}, "
        f"deduped {dropped} ({len(new_keys)} newly marked seen)"
    )


def main() -> None:
    settings = AppSettings()

    consumer = build_consumer(settings.input)

    dlq_producer: Producer | None = None
    if settings.dlq is not None:
        dlq_producer = build_producer(settings.dlq)

    producer = build_producer(settings.output, dlq_producer=dlq_producer)

    store = BucketedDedupStore(
        root_dir=settings.dedup.store_dir,
        window_hours=settings.dedup.window_hours,
        bucket_hours=settings.dedup.bucket_hours,
        tuning=settings.dedup.rocksdb,
    )

    stats = _new_stats()
    # Same numbers as the periodic perf log line, as Prometheus counters.
    start_metrics_server(settings.metrics, stats)
    try:
        while True:
            run_one_iteration(consumer, producer, store, settings, stats)
            stats.report_if_due()
    finally:
        consumer.close()
        # Before the DLQ producer: the output can still route rows to it.
        producer.close()
        if dlq_producer is not None:
            dlq_producer.close()
        store.close()
