from loguru import logger
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

from tkati_node_el.settings import AppSettings

# Reported in this order, not sorted by duration: a stable field order is what
# makes two consecutive log lines comparable at a glance. The consumer's and
# producer's phases are spliced in from tkati-core, which owns the names it
# times itself against; `commit` is the only phase this node times itself.
_PHASES = (*CONSUMER_PHASES, *PRODUCER_PHASES, "commit")


def _new_stats() -> LoopStats:
    return LoopStats(phases=_PHASES)


def run_one_iteration(
    consumer: Consumer,
    producer: Producer,
    settings: AppSettings,
    stats: LoopStats | None = None,
) -> None:
    stats = stats if stats is not None else _new_stats()

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

    # A short batch means the node drained the topic and waited out the batch
    # timeout — it wasn't CPU-bound, so its timings say nothing about whether
    # this node can keep up.
    if len(batch) < settings.input.consumer.batch_size:
        stats.starved_iterations += 1
    stats.rows_in += len(batch)

    # No phase blocks here either: the producer splits its own time into
    # `serialize`, `enqueue` and `deliver`, for the same reason as the
    # consumer above.
    producer.produce_arrow(batch, stats=stats)
    # Block until actually delivered before committing: KafkaProducer's
    # produce_arrow() only enqueues, so committing straight after it would lose
    # the batch on a crash while its offset says it was handled.
    # ClickhouseProducer.flush() is a no-op since its inserts are already
    # synchronous.
    producer.flush(stats=stats)
    stats.rows_out += len(batch)

    # Only after a confirmed delivery: commit. If we crash before this line,
    # the batch is re-read at restart and written again — a duplicate at
    # worst, never a lost event.
    with stats.phase("commit"):
        consumer.commit()

    logger.debug(f"Produced {len(batch)} rows")


def main() -> None:
    settings = AppSettings()

    consumer = build_consumer(settings.input)

    dlq_producer: Producer | None = None
    if settings.dlq is not None:
        dlq_producer = build_producer(settings.dlq)

    producer = build_producer(settings.output, dlq_producer=dlq_producer)

    stats = _new_stats()
    # Same numbers as the periodic perf log line, as Prometheus counters.
    start_metrics_server(settings.metrics, stats)
    try:
        while True:
            run_one_iteration(consumer, producer, settings, stats)
            stats.report_if_due()
    finally:
        consumer.close()
        # Before the DLQ producer: the output can still route rows to it.
        producer.close()
        if dlq_producer is not None:
            dlq_producer.close()
