from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from tkati_core.clickhouse.producer import ClickhouseProducer, _insert_with_dlq_fallback, _insert_with_retry


def _make_arrow_table(n: int = 1) -> pa.Table:
    return pa.table({"uid": [f"uid-{i}" for i in range(n)], "traffic_in": [100 + i for i in range(n)]})


def test_insert_retry_on_failure() -> None:
    """CH fails twice then succeeds: insert_arrow called 3x."""
    ch_client = MagicMock()
    arrow_table = _make_arrow_table()

    ch_client.insert_arrow.side_effect = [
        Exception("CH unavailable"),
        Exception("CH unavailable"),
        None,
    ]

    with patch("time.sleep"):
        _insert_with_retry(ch_client=ch_client, table="traffic_event", arrow_table=arrow_table)

    assert ch_client.insert_arrow.call_count == 3


def test_insert_retry_all_fail() -> None:
    """CH always fails: exception raised after 3 attempts."""
    ch_client = MagicMock()
    arrow_table = _make_arrow_table()

    ch_client.insert_arrow.side_effect = Exception("CH always down")

    with patch("time.sleep"):
        with pytest.raises(Exception, match="CH always down"):
            _insert_with_retry(ch_client=ch_client, table="traffic_event", arrow_table=arrow_table)

    assert ch_client.insert_arrow.call_count == 3


def test_fallback_all_succeed() -> None:
    """Large batch always fails; sub-chunks all succeed. DLQ never written."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()
    arrow_table = _make_arrow_table(4)

    def insert_side_effect(table, arrow_table):
        if len(arrow_table) == 4:
            raise Exception("full batch rejected")

    ch_client.insert_arrow.side_effect = insert_side_effect

    ch_producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event", dlq_producer=dlq_producer, split_factor=2)

    with patch("time.sleep"):
        ch_producer.produce_arrow(arrow_table)

    dlq_producer.produce_arrow.assert_not_called()
    dlq_producer.flush.assert_called_once()


def test_dlq_single_bad_row() -> None:
    """A single bad row always rejected by CH → written to DLQ once."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()

    bad_row = _make_arrow_table(1)
    ch_client.insert_arrow.side_effect = Exception("bad row")

    with patch("time.sleep"):
        _insert_with_dlq_fallback(
            table=bad_row,
            ch_client=ch_client,
            ch_table="traffic_event",
            dlq_producer=dlq_producer,
            split_factor=10,
        )

    dlq_producer.produce_arrow.assert_called_once()
    sent = dlq_producer.produce_arrow.call_args[0][0]
    assert len(sent) == 1


def test_no_dlq_raises_on_failure() -> None:
    """Without DLQ configured, a CH failure propagates."""
    ch_client = MagicMock()
    ch_client.insert_arrow.side_effect = Exception("CH down")

    ch_producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event", dlq_producer=None)

    with patch("time.sleep"):
        with pytest.raises(Exception, match="CH down"):
            ch_producer.produce_arrow(_make_arrow_table(2))


def test_recursive_descent() -> None:
    """4-row batch fails; with split_factor=2, recursion finds and DLQs exactly the one bad row."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()

    def insert_side_effect(table, arrow_table):
        if "uid-2" in arrow_table.column("uid").to_pylist():
            raise Exception("batch contains bad row uid-2")

    ch_client.insert_arrow.side_effect = insert_side_effect

    arrow_table = _make_arrow_table(4)  # uid-0, uid-1, uid-2, uid-3

    with patch("time.sleep"):
        _insert_with_dlq_fallback(
            table=arrow_table,
            ch_client=ch_client,
            ch_table="traffic_event",
            dlq_producer=dlq_producer,
            split_factor=2,
        )

    dlq_producer.produce_arrow.assert_called_once()
    sent = dlq_producer.produce_arrow.call_args[0][0]
    assert len(sent) == 1
    assert sent.column("uid")[0].as_py() == "uid-2"


def test_ch_producer_success_no_dlq_call() -> None:
    """produce_arrow succeeds: insert called once, DLQ never touched."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()
    arrow_table = _make_arrow_table(3)

    producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event", dlq_producer=dlq_producer)
    producer.produce_arrow(arrow_table)

    ch_client.insert_arrow.assert_called_once()
    dlq_producer.produce_arrow.assert_not_called()
    dlq_producer.flush.assert_not_called()


def test_ch_producer_failure_with_dlq() -> None:
    """produce_arrow fails: recursive fallback runs and DLQ is flushed."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()
    arrow_table = _make_arrow_table(1)
    ch_client.insert_arrow.side_effect = Exception("CH down")

    producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event", dlq_producer=dlq_producer)

    with patch("time.sleep"):
        producer.produce_arrow(arrow_table)

    dlq_producer.produce_arrow.assert_called_once()
    dlq_producer.flush.assert_called_once()


def test_ch_producer_failure_without_dlq_raises() -> None:
    """produce_arrow fails with no DLQ configured: exception propagates."""
    ch_client = MagicMock()
    arrow_table = _make_arrow_table(1)
    ch_client.insert_arrow.side_effect = Exception("CH down")

    producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event", dlq_producer=None)

    with patch("time.sleep"):
        with pytest.raises(Exception, match="CH down"):
            producer.produce_arrow(arrow_table)


def test_ch_producer_as_dlq_for_another_ch_producer() -> None:
    """A ClickhouseProducer can itself be used as the dlq_producer for another ClickhouseProducer."""
    primary_ch_client = MagicMock()
    dlq_ch_client = MagicMock()
    arrow_table = _make_arrow_table(1)

    primary_ch_client.insert_arrow.side_effect = Exception("CH down")

    dlq_producer = ClickhouseProducer(ch_client=dlq_ch_client, table="traffic_event_dlq")
    producer = ClickhouseProducer(
        ch_client=primary_ch_client, table="traffic_event", dlq_producer=dlq_producer
    )

    with patch("time.sleep"):
        producer.produce_arrow(arrow_table)

    dlq_ch_client.insert_arrow.assert_called_once()


def test_ch_producer_flush_is_noop() -> None:
    """flush() on ClickhouseProducer is a no-op and never touches ch_client."""
    ch_client = MagicMock()
    ClickhouseProducer(ch_client=ch_client, table="traffic_event").flush()
    ch_client.assert_not_called()


def test_ch_producer_produce_pylist() -> None:
    """produce_pylist converts rows to an Arrow table and inserts them."""
    ch_client = MagicMock()
    rows = [{"uid": "uid-0", "traffic_in": 100}, {"uid": "uid-1", "traffic_in": 101}]

    producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event")
    producer.produce_pylist(rows)

    ch_client.insert_arrow.assert_called_once()
    sent = ch_client.insert_arrow.call_args.kwargs["arrow_table"]
    assert sent.to_pylist() == rows


def test_ch_producer_close() -> None:
    """close() delegates to the underlying ch_client."""
    ch_client = MagicMock()
    ClickhouseProducer(ch_client=ch_client, table="traffic_event").close()
    ch_client.close.assert_called_once()
