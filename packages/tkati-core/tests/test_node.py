"""The harness's guarantees, against the in-memory doubles. No broker."""

import os
import signal
import time

import pyarrow as pa
import pytest
from pydantic import ValidationError
from tkati_core import (
    CONSUMER_PHASES,
    DEFAULT_PHASES,
    PRODUCER_PHASES,
    SINK_PHASES,
    Batch,
    ConsumedBatch,
    Idle,
    LoopStats,
    Node,
    NodeSettings,
)
from tkati_core.clickhouse.settings import (
    ClickHouseConnectionSettings,
    ClickHouseOutputSettings,
    ClickHouseTableSettings,
)
from tkati_core.kafka.settings import (
    KafkaConnectionSettings,
    KafkaConsumerSettings,
    KafkaInputSettings,
    KafkaTopicSettings,
)
from tkati_core.testing import MemoryConsumer, MemoryProducer, memory_node


def _table(n: int) -> pa.Table:
    return pa.table({"uid": [str(i) for i in range(n)]})


def _send_all(node: Node) -> list[type]:
    """The simplest node: send every batch unchanged. Returns the event types
    it saw."""
    seen: list[type] = []
    for event in node.consume_arrow():
        seen.append(type(event))
        if isinstance(event, Batch):
            node.done(event, output_arrow=event.data)
    return seen


def _raise_in_body(node: Node, exc: BaseException) -> None:
    for _ in node.consume_arrow():
        raise exc


def _skip_done(node: Node) -> None:
    """A buggy node: never finishes the batch."""
    for event in node.consume_arrow():
        assert isinstance(event, Batch)


def _producer(producer: MemoryProducer | None) -> MemoryProducer:
    assert producer is not None
    return producer


# --- finishing a batch -----------------------------------------------------


def test_done_flushes_then_commits() -> None:
    node, consumer, producer = memory_node([_table(2), _table(3)])
    with node:
        _send_all(node)

    assert consumer.log == [
        "read:0",
        "produce",
        "flush",
        "commit:0",
        "read:1",
        "produce",
        "flush",
        "commit:1",
        "close:consumer",
        "close:producer",
    ]
    assert [len(t) for t in _producer(producer).sent] == [2, 3]
    assert consumer.rewinds == []


def test_code_after_done_runs_after_the_commit() -> None:
    node, consumer, _ = memory_node([_table(1)])
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            node.done(event)
            consumer.log.append("tail")

    assert consumer.log[:4] == ["read:0", "flush", "commit:0", "tail"]


def test_rows_out_is_counted_at_done() -> None:
    node, _, _ = memory_node([_table(2)])
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            assert node.stats.rows_out == 0
            node.done(event, output_arrow=event.data)
            assert node.stats.rows_out == 2


def test_next_event_without_done_raises_and_rewinds() -> None:
    node, consumer, _ = memory_node([_table(1), _table(1)])
    with pytest.raises(RuntimeError, match="batch 0 was not marked done"), node:
        _skip_done(node)

    assert consumer.commits == []
    assert consumer.rewinds == [0]
    assert "read:1" not in consumer.log


def test_done_twice_raises() -> None:
    node, consumer, _ = memory_node([_table(1)])

    def done_twice() -> None:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            node.done(event)
            node.done(event)

    with pytest.raises(RuntimeError, match="already called"), node:
        done_twice()

    assert consumer.commits == [0]
    assert consumer.rewinds == []


def test_done_with_both_output_and_rows_out_raises_and_rewinds() -> None:
    node, consumer, producer = memory_node([_table(1)])

    def both() -> None:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            node.done(event, output_arrow=event.data, rows_out=1)

    with pytest.raises(ValueError, match="at most one"), node:
        both()

    assert _producer(producer).sent == []
    assert consumer.commits == []
    assert consumer.rewinds == [0]


def test_failed_send_in_done_rewinds() -> None:
    class _FailingProducer(MemoryProducer):
        def produce_arrow(self, data, stats=None) -> None:
            raise RuntimeError("produce failed")

    consumer = MemoryConsumer([_table(1)])
    node = Node(consumer, _FailingProducer(), batch_size=100, batch_timeout_sec=0)
    with pytest.raises(RuntimeError, match="produce failed"), node:
        _send_all(node)

    assert consumer.commits == []
    assert consumer.rewinds == [0]


def test_exception_in_the_body_rewinds_and_propagates() -> None:
    node, consumer, producer = memory_node([_table(1), _table(1)])
    with pytest.raises(RuntimeError, match="boom"), node:
        _raise_in_body(node, RuntimeError("boom"))

    assert consumer.commits == []
    assert consumer.rewinds == [0]
    assert consumer.closed and _producer(producer).closed
    assert node.stats.rows_out == 0


def test_exception_after_done_does_not_rewind_the_committed_batch() -> None:
    node, consumer, _ = memory_node([_table(1)])

    def fail_after_done() -> None:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            node.done(event)
            raise RuntimeError("tail failed")

    with pytest.raises(RuntimeError, match="tail failed"), node:
        fail_after_done()

    assert consumer.commits == [0]
    assert consumer.rewinds == []


def test_failed_flush_in_done_rewinds_instead_of_committing() -> None:
    node, consumer, _ = memory_node([_table(1)], fail_flush=RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"), node:
        _send_all(node)

    assert consumer.commits == []
    assert consumer.rewinds == [0]


class _BrokenRewind(MemoryConsumer):
    def rewind(self, batch: ConsumedBatch) -> None:
        raise ValueError("rewind failed")


def test_failing_rewind_does_not_mask_the_original_error() -> None:
    consumer = _BrokenRewind([_table(1)])
    node = Node(consumer, MemoryProducer(), batch_size=100, batch_timeout_sec=0)
    with pytest.raises(RuntimeError, match="boom"), node:
        _raise_in_body(node, RuntimeError("boom"))
    assert consumer.closed


def test_break_before_done_neither_commits_nor_rewinds() -> None:
    node, consumer, producer = memory_node([_table(1), _table(1)])
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            break

    assert consumer.commits == []
    assert consumer.rewinds == []
    assert consumer.closed and _producer(producer).closed


def test_break_after_done_keeps_the_commit() -> None:
    node, consumer, _ = memory_node([_table(1), _table(1)])
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            node.done(event)
            break

    assert consumer.commits == [0]
    assert consumer.rewinds == []


def test_keyboard_interrupt_does_not_rewind() -> None:
    """A forced stop shouldn't wait on a seek; the batch is uncommitted
    either way."""
    node, consumer, _ = memory_node([_table(1)])
    with pytest.raises(KeyboardInterrupt), node:
        _raise_in_body(node, KeyboardInterrupt())

    assert consumer.commits == []
    assert consumer.rewinds == []
    assert consumer.closed


# --- events -----------------------------------------------------------------


def test_empty_poll_yields_idle() -> None:
    node, consumer, _ = memory_node([None, _table(1)])
    with node:
        seen = _send_all(node)

    assert seen == [Idle, Batch]
    assert consumer.commits == [0]


def test_stop_when_idle_ends_the_loop_at_the_first_empty_poll() -> None:
    consumer = MemoryConsumer([_table(1), None, _table(1)])
    node = Node(
        consumer,
        MemoryProducer(),
        batch_size=100,
        batch_timeout_sec=0,
        stop_when_idle=True,
    )
    with node:
        seen = _send_all(node)

    assert seen == [Batch]
    assert consumer.commits == [0]


def test_short_batch_is_flagged() -> None:
    node, _, _ = memory_node([_table(2), _table(1)], batch_size=2)
    shorts: list[bool] = []
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            shorts.append(event.short)
            node.done(event)
    assert shorts == [False, True]


def test_empty_table_is_not_sent() -> None:
    node, consumer, producer = memory_node([_table(1)])
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            node.done(event, output_arrow=event.data.slice(0, 0))

    assert _producer(producer).sent == []
    assert consumer.commits == [0]


def test_stop_ends_the_loop_after_the_current_batch() -> None:
    node, consumer, _ = memory_node([_table(1), _table(1)])
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            node.stop()
            node.done(event, output_arrow=event.data)

    assert consumer.commits == [0]
    assert "read:1" not in consumer.log


def test_consuming_outside_with_raises() -> None:
    node, _, _ = memory_node([_table(1)])
    with pytest.raises(RuntimeError, match="inside `with node:`"):
        node.consume_arrow()
    with pytest.raises(RuntimeError, match="inside `with node:`"):
        node.consume_pylist()


def test_node_itself_is_not_iterable() -> None:
    """The input format is always chosen explicitly."""
    node, _, _ = memory_node([_table(1)])
    with pytest.raises(TypeError), node:
        iter(node)  # ty: ignore[no-matching-overload]  # the point of the test


# --- stats ------------------------------------------------------------------


def test_consumer_and_producer_are_handed_the_node_stats() -> None:
    """The harness times no read or produce phase of its own; it relies on the
    consumer and producer to fill in theirs."""
    node, consumer, producer = memory_node([_table(1)])
    with node:
        _send_all(node)

    assert consumer.stats_seen and all(s is node.stats for s in consumer.stats_seen)
    stats_seen = _producer(producer).stats_seen
    assert stats_seen and all(s is node.stats for s in stats_seen)


def test_rows_and_starved_iterations_are_counted() -> None:
    """A full batch is not starved; a short batch or an empty poll is."""
    node, _, _ = memory_node([_table(2), _table(1), None], batch_size=2)
    with node:
        _send_all(node)

    stats: LoopStats = node.stats
    assert stats.iterations == 3
    assert stats.starved_iterations == 2
    assert stats.rows_in == 3
    assert stats.rows_out == 3


def test_node_phases_are_timed() -> None:
    phases = (*CONSUMER_PHASES, "work", *PRODUCER_PHASES, "commit")
    node, _, _ = memory_node([_table(1)], phases=phases)
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            with node.phase("work"):
                time.sleep(0.002)
            node.done(event)

    assert node.stats.phases == phases
    assert node.stats.phase_sec["work"] > 0
    assert "commit" in node.stats.phase_sec


def test_phases_missing_a_harness_phase_raise() -> None:
    with pytest.raises(ValueError, match="producer/deliver"):
        memory_node([], phases=(*CONSUMER_PHASES, "commit"))


def test_default_phases() -> None:
    node, _, _ = memory_node([])
    assert node.stats.phases == DEFAULT_PHASES
    node, _, _ = memory_node([], output=False)
    assert node.stats.phases == SINK_PHASES


# --- closing ----------------------------------------------------------------


class _BrokenClose(MemoryConsumer):
    def close(self) -> None:
        super().close()
        raise RuntimeError("close failed")


def test_close_order_is_consumer_producer_dlq_and_survives_a_failing_close() -> None:
    log: list[str] = []
    consumer = _BrokenClose([], log=log)
    producer = MemoryProducer(log=log)
    dlq = MemoryProducer(log=log, name="dlq")
    node = Node(consumer, producer, batch_size=1, batch_timeout_sec=0, dlq=dlq)
    consumer.on_exhausted = node.stop
    with pytest.raises(RuntimeError, match="close failed"), node:
        _send_all(node)

    assert log == ["close:consumer", "close:producer", "close:dlq"]


# --- without an output producer ---------------------------------------------


def test_without_a_producer_the_batch_is_committed_without_a_flush() -> None:
    node, consumer, producer = memory_node([_table(2)], output=False)
    assert producer is None
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            node.done(event, rows_out=len(event.data))

    assert consumer.log == ["read:0", "commit:0", "close:consumer"]
    assert node.stats.rows_out == 2


def test_without_a_producer_output_raises_and_the_batch_is_rewound() -> None:
    node, consumer, _ = memory_node([_table(1)], output=False)
    with pytest.raises(RuntimeError, match="no output producer"), node:
        _send_all(node)

    assert consumer.rewinds == [0]


def test_without_a_producer_phases_need_not_include_the_producer_phases() -> None:
    phases = (*CONSUMER_PHASES, "upload", "commit")
    node, _, _ = memory_node([], output=False, phases=phases)
    assert node.stats.phases == phases


# --- pylist ----------------------------------------------------------------


def _rows(n: int) -> list[dict]:
    return [{"uid": str(i)} for i in range(n)]


def test_consume_pylist_yields_dicts_and_sends_them_with_produce_pylist() -> None:
    node, consumer, producer = memory_node([_rows(2)])
    seen: list[list[dict]] = []
    with node:
        for event in node.consume_pylist():
            assert isinstance(event, Batch)
            seen.append(event.data)
            node.done(event, output_pylist=event.data)

    assert seen == [_rows(2)]
    assert _producer(producer).sent == [_rows(2)]
    assert consumer.log[:4] == ["read:0", "produce", "flush", "commit:0"]


def test_pylist_input_may_send_arrow() -> None:
    node, _, producer = memory_node([_rows(2)])
    with node:
        for event in node.consume_pylist():
            assert isinstance(event, Batch)
            node.done(event, output_arrow=pa.Table.from_pylist(event.data))

    sent = _producer(producer).sent
    assert len(sent) == 1 and isinstance(sent[0], pa.Table)


def test_arrow_input_may_send_pylist() -> None:
    node, _, producer = memory_node([_table(2)])
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            node.done(event, output_pylist=event.data.to_pylist())

    assert _producer(producer).sent == [_rows(2)]


def test_pylist_batches_are_counted() -> None:
    node, _, _ = memory_node([_rows(2), _rows(1)], batch_size=2)
    shorts: list[bool] = []
    with node:
        for event in node.consume_pylist():
            assert isinstance(event, Batch)
            shorts.append(event.short)
            node.done(event, output_pylist=event.data[:1])

    assert shorts == [False, True]
    assert node.stats.rows_in == 3
    assert node.stats.rows_out == 2


def test_both_outputs_raise_and_rewind() -> None:
    node, consumer, producer = memory_node([_rows(1)])

    def both() -> None:
        for event in node.consume_pylist():
            assert isinstance(event, Batch)
            node.done(
                event,
                output_arrow=pa.Table.from_pylist(event.data),
                output_pylist=event.data,
            )

    with pytest.raises(ValueError, match="at most one"), node:
        both()

    assert _producer(producer).sent == []
    assert consumer.commits == []
    assert consumer.rewinds == [0]


def test_without_a_producer_output_pylist_raises() -> None:
    node, consumer, _ = memory_node([_rows(1)], output=False)

    def send() -> None:
        for event in node.consume_pylist():
            assert isinstance(event, Batch)
            node.done(event, output_pylist=event.data)

    with pytest.raises(RuntimeError, match="no output producer"), node:
        send()

    assert consumer.rewinds == [0]


# --- signals ----------------------------------------------------------------


def _signal_node(consumer: MemoryConsumer) -> Node:
    node = Node(
        consumer,
        MemoryProducer(log=consumer.log),
        batch_size=100,
        batch_timeout_sec=0,
        handle_signals=True,
    )
    consumer.on_exhausted = node.stop
    return node


def test_sigterm_in_the_body_stops_after_committing_the_batch() -> None:
    consumer = MemoryConsumer([_table(1), _table(1)])
    node = _signal_node(consumer)
    with node:
        for event in node.consume_arrow():
            assert isinstance(event, Batch)
            os.kill(os.getpid(), signal.SIGTERM)
            node.done(event, output_arrow=event.data)

    assert consumer.commits == [0]
    assert "read:1" not in consumer.log


class _SlowConsumer(MemoryConsumer):
    """A read that blocks, as a poll waiting out the batch timeout does, and
    receives SIGTERM while blocked."""

    def read_arrow(self, timeout, num_messages, stats=None):
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(5)
        return super().read_arrow(timeout, num_messages, stats)


def test_sigterm_during_a_read_cuts_it_short() -> None:
    consumer = _SlowConsumer([_table(1)])
    node = _signal_node(consumer)
    started = time.monotonic()
    with node:
        seen = _send_all(node)

    assert time.monotonic() - started < 2
    assert seen == []
    assert consumer.commits == []


def test_second_signal_forces_a_stop() -> None:
    consumer = MemoryConsumer([_table(1)])
    node = _signal_node(consumer)

    def signal_twice() -> None:
        for _ in node.consume_arrow():
            os.kill(os.getpid(), signal.SIGTERM)
            os.kill(os.getpid(), signal.SIGTERM)

    with pytest.raises(KeyboardInterrupt), node:
        signal_twice()

    assert consumer.commits == []


def test_previous_signal_handlers_are_restored() -> None:
    before = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    node = _signal_node(MemoryConsumer([]))
    with node:
        assert signal.getsignal(signal.SIGTERM) != before[signal.SIGTERM]
        _send_all(node)
    assert {sig: signal.getsignal(sig) for sig in before} == before


# --- settings ---------------------------------------------------------------


def _input() -> KafkaInputSettings:
    return KafkaInputSettings(
        connection=KafkaConnectionSettings(broker="localhost:9092"),
        topic=KafkaTopicSettings(name="in", schema={"uid": "string"}),
        consumer=KafkaConsumerSettings(group_id="g"),
    )


def _clickhouse() -> ClickHouseOutputSettings:
    return ClickHouseOutputSettings(
        connection=ClickHouseConnectionSettings(host="localhost"),
        table=ClickHouseTableSettings(database="default", name="t"),
    )


def test_node_settings_output_is_optional() -> None:
    settings = NodeSettings(input=_input())
    assert settings.output is None
    assert settings.dlq is None


def test_node_settings_reject_dlq_without_output() -> None:
    with pytest.raises(ValidationError, match="dlq"):
        NodeSettings(input=_input(), dlq=_clickhouse())


def test_from_settings_without_output_builds_no_producer(monkeypatch) -> None:
    built: list[object] = []
    monkeypatch.setattr(
        "tkati_core.node.build_consumer",
        lambda s: built.append(s) or MemoryConsumer([]),
    )

    def no_producer(*_args, **_kwargs):
        raise AssertionError("no producer should be built")

    monkeypatch.setattr("tkati_core.node.build_producer", no_producer)

    node = Node.from_settings(NodeSettings(input=_input()))
    assert node.stats.phases == SINK_PHASES
    assert len(built) == 1
