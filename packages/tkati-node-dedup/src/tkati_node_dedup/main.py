import pyarrow as pa
from loguru import logger
from tkati_core import Consumer, Producer, build_consumer, build_producer

from tkati_node_dedup.settings import AppSettings
from tkati_node_dedup.store import BucketedDedupStore


def _dedupe_batch(
    batch: pa.Table, field: str, store: BucketedDedupStore
) -> tuple[pa.Table, list[bytes]]:
    """Filter out rows whose dedup key was already seen (in-batch or in the store).

    Returns (filtered_batch, keys_to_mark_seen). Null values in `field` always
    pass through and are never added to the store — we can't dedup on nothing.

    Encoding and the store lookup are both batched (one pass over the column,
    one RocksDB round trip per open bucket) rather than done per row.
    """
    keys = store.encode_keys(batch.column(field))
    keep_mask, new_keys = store.filter_duplicates(keys)
    filtered = batch.filter(keep_mask)
    return filtered, new_keys


def run_one_iteration(
    consumer: Consumer,
    producer: Producer,
    store: BucketedDedupStore,
    settings: AppSettings,
) -> None:
    # Runs first, every iteration (even if no batch arrives), and can never
    # raise. Buckets must be fresh *before* the dedupe check below runs —
    # doing this only after commit would leave a just-expired bucket open and
    # checked against for one extra iteration, and an idle node (no messages,
    # read_arrow returns None below) would never clean up at all.
    try:
        store.cleanup_expired()
    except Exception:
        logger.exception("dedup store cleanup failed; will retry next iteration")

    batch = consumer.read_arrow(
        num_messages=settings.input.consumer.batch_size,
        timeout=settings.input.consumer.batch_timeout_sec,
    )
    if batch is None:
        return

    field = settings.dedup.field
    if field not in batch.column_names:
        logger.warning(
            f"Dedup field '{field}' missing from batch schema; passing batch through unfiltered"
        )
        filtered, new_keys = batch, []
    else:
        filtered, new_keys = _dedupe_batch(batch, field, store)

    dropped = len(batch) - len(filtered)

    if len(filtered) > 0:
        producer.produce_arrow(filtered)
        # Block until actually delivered before marking anything "seen" or
        # committing. Required here even though tkati-node-el's loop skips it:
        # KafkaProducer.produce_arrow() only enqueues (non-blocking), and
        # marking a key seen before it's durably delivered would risk losing
        # the event permanently on a crash. ClickhouseProducer.flush() is a
        # no-op since its inserts are already synchronous.
        producer.flush()

    # Only after a confirmed-successful produce: mark these keys seen.
    store.add_many(new_keys)

    # Only after mark-seen: commit. If we crash before this line, the batch is
    # re-read at restart; those keys are already in the store, so re-processing
    # it drops what was already produced — a harmless duplicate at worst, never
    # a lost event.
    consumer.commit()

    logger.info(
        f"Batch of {len(batch)} rows: produced {len(filtered)}, "
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
    )

    try:
        while True:
            run_one_iteration(consumer, producer, store, settings)
    finally:
        consumer.close()
        if dlq_producer is not None:
            dlq_producer.close()
        store.close()
