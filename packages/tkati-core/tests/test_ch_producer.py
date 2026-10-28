from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

import clickhouse_connect.driver.exceptions as ch_exc
import pyarrow as pa
import pytest
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.models import SettingDef
from tkati_core import PRODUCER_PHASES, LoopStats
from tkati_core.clickhouse.producer import (
    CH_DATA_ERROR_CODES,
    ClickhouseProducer,
    _insert_with_dlq_fallback,
    _insert_with_retry,
    _is_data_error,
)
from tkati_core.clickhouse.settings import (
    ClickHouseConnectionSettings,
    ClickHouseOutputSettings,
    ClickHouseTableSettings,
)
from urllib3 import PoolManager
from urllib3.exceptions import ProtocolError
from urllib3.response import HTTPResponse

# Mock only this retry controller's wait, leaving other threads' clocks intact.
_RETRY_SLEEP = "tkati_core.clickhouse.producer._insert_with_retry.retry.sleep"


def _make_arrow_table(n: int = 1) -> pa.Table:
    return pa.table({"uid": [f"uid-{i}" for i in range(n)], "traffic_in": [100 + i for i in range(n)]})


def _ch_data_error(code: int = 376, name: str = "CANNOT_PARSE_UUID") -> ch_exc.DatabaseError:
    """What the driver raises when the server rejects the rows. The class carries
    no meaning, the `code` does: `HttpClient._error_handler` picks `DatabaseError`
    or `OperationalError` by whether the request was retried."""
    return ch_exc.DatabaseError(
        f"Received ClickHouse exception, code: {code}, server response: "
        f"Code: {code}. DB::Exception: Cannot parse uuid 'nope'. ({name})",
        code=code,
        name=name,
    )


def _ch_transport_error() -> ch_exc.OperationalError:
    """A pure transport failure: `OperationalError` with no server code at all."""
    return ch_exc.OperationalError(
        "Error executing HTTP request attempt 1 (http://localhost:8123): Connection refused"
    )


def test_timeout_after_commit_retries_without_duplicate_or_session_lock() -> None:
    """A committed insert loses its reply; its retry must not insert it again.

    Exercise the real driver, replacing server discovery and HTTP transport.
    A separate call with identical data and the same delivery tag is a new
    logical insert and must still be written.
    """
    output = ClickHouseOutputSettings(
        connection=ClickHouseConnectionSettings(host="localhost", port=8123),
        table=ClickHouseTableSettings(name="traffic_event"),
    )
    requests: list[dict[str, list[str]]] = []
    committed_tokens: set[str] = set()
    locked_sessions: set[str] = set()
    landed: list[dict] = []

    def discover(client: Client, tz_source: str) -> None:
        client.server_settings = {
            "insert_deduplicate": SettingDef("insert_deduplicate", "0", 0),
            "insert_deduplication_token": SettingDef("insert_deduplication_token", "", 0),
        }

    def request(method: str, url: str, **kwargs) -> HTTPResponse:
        params = parse_qs(urlsplit(url).query)
        requests.append(params)
        session = params.get("session_id", [None])[0]
        if session in locked_sessions:
            return HTTPResponse(
                status=500,
                headers={"X-ClickHouse-Exception-Code": "373"},
                body=b"Code: 373. Session is locked by a concurrent client. (SESSION_IS_LOCKED)",
            )
        token = params.get("insert_deduplication_token", [None])[0]
        if token is None or token not in committed_tokens:
            landed.extend(pa.ipc.open_file(kwargs["body"]).read_all().to_pylist())
            if token is not None:
                committed_tokens.add(token)
        if len(requests) == 1:
            if session is not None:
                locked_sessions.add(session)
            raise ProtocolError("Connection aborted.", TimeoutError("timed out"))
        return HTTPResponse(status=200, body=b"")

    with (
        patch.object(Client, "_init_common_settings", new=discover),
        patch.object(PoolManager, "request", side_effect=request),
        patch(_RETRY_SLEEP),
    ):
        producer = ClickhouseProducer.from_output_settings(output)
        data = _make_arrow_table()
        try:
            producer.produce_arrow(data, tag=1)
            producer.produce_arrow(data, tag=1)
        finally:
            producer.close()

    assert landed == data.to_pylist() * 2
    assert len(requests) == 3
    assert all("session_id" not in params for params in requests)
    assert all(params["insert_deduplicate"] == ["1"] for params in requests)
    tokens = [params["insert_deduplication_token"][0] for params in requests]
    assert tokens[0] == tokens[1]
    assert tokens[0] != tokens[2]


def test_dlq_split_tokens_do_not_drop_identical_rows_or_duplicate_on_timeout() -> None:
    """Each split is an independent insert, even when rows are identical."""
    ch_client = MagicMock()
    dlq = MagicMock()
    attempts: list[tuple[list[str], str]] = []
    committed: dict[str, list[str]] = {}

    def insert(table, arrow_table, settings):
        rows = arrow_table.column("uid").to_pylist()
        token = settings["insert_deduplication_token"]
        attempts.append((rows, token))
        if "bad" in rows:
            raise _ch_data_error()
        if token in committed:
            return
        committed[token] = rows
        if len(rows) == 2:
            # The last chunk commits, then loses its acknowledgement.
            raise _ch_transport_error()

    ch_client.insert_arrow.side_effect = insert
    producer = ClickhouseProducer(ch_client, "traffic_event", dlq, split_factor=2)
    with patch(_RETRY_SLEEP) as sleep:
        producer.produce_arrow(pa.table({"uid": ["same", "bad", "same", "same"]}))

    assert [uid for rows in committed.values() for uid in rows] == ["same"] * 3
    dlq.produce_arrow.assert_called_once()
    assert dlq.produce_arrow.call_args.args[0].column("uid").to_pylist() == ["bad"]
    dlq.flush.assert_called_once()
    sleep.assert_called_once()
    # produce_arrow and the entry to the fallback resend the same full batch.
    assert attempts[0] == attempts[1]
    # The timed-out child keeps its own identity on retry.
    assert attempts[-2] == attempts[-1]
    assert attempts[-1][1] != attempts[0][1]


def test_produce_pylist_reuses_token_across_retries() -> None:
    ch_client = MagicMock()
    ch_client.insert_arrow.side_effect = [_ch_transport_error(), None]
    producer = ClickhouseProducer(ch_client, "traffic_event")
    with patch(_RETRY_SLEEP):
        producer.produce_pylist([{"uid": "uid-0"}])

    calls = ch_client.insert_arrow.call_args_list
    assert len(calls) == 2
    assert calls[0].kwargs["settings"] == calls[1].kwargs["settings"]
    assert calls[0].kwargs["settings"]["insert_deduplication_token"]


@pytest.mark.parametrize(
    ("err", "expected"),
    [
        (_ch_data_error(376, "CANNOT_PARSE_UUID"), True),
        (_ch_data_error(117, "INCORRECT_DATA"), True),
        # Schema drift is deliberately not a data error: it rejects every row
        # alike, so it must stop the node rather than drain the batch to the DLQ.
        (_ch_data_error(53, "TYPE_MISMATCH"), False),
        (_ch_data_error(16, "NO_SUCH_COLUMN_IN_TABLE"), False),
        # A code the driver couldn't read, whichever class it arrives as.
        (_ch_transport_error(), False),
        (ch_exc.DatabaseError("no code in the header", code=None), False),
        # Not a driver error at all, e.g. pyarrow failing to serialize.
        (ValueError("boom"), False),
    ],
)
def test_is_data_error_keys_off_the_server_code(err: BaseException, expected: bool) -> None:
    """The exception class says nothing; the ClickHouse error code says everything."""
    assert _is_data_error(err) is expected
    assert 53 not in CH_DATA_ERROR_CODES
    assert 376 in CH_DATA_ERROR_CODES


def test_a_data_error_is_not_retried() -> None:
    """A bad row will never parse, so it costs one insert and no sleep."""
    ch_client = MagicMock()
    ch_client.insert_arrow.side_effect = _ch_data_error()

    with patch(_RETRY_SLEEP) as sleep:
        with pytest.raises(ch_exc.DatabaseError):
            _insert_with_retry(ch_client=ch_client, table="traffic_event", arrow_table=_make_arrow_table(), deduplication_token="insert-1")

    assert ch_client.insert_arrow.call_count == 1
    sleep.assert_not_called()


def test_insert_retry_on_non_data_error() -> None:
    """CH unreachable twice then back: insert_arrow called 3x."""
    ch_client = MagicMock()
    arrow_table = _make_arrow_table()

    ch_client.insert_arrow.side_effect = [
        _ch_transport_error(),
        _ch_transport_error(),
        None,
    ]

    with patch(_RETRY_SLEEP):
        _insert_with_retry(ch_client=ch_client, table="traffic_event", arrow_table=arrow_table, deduplication_token="insert-1")

    assert ch_client.insert_arrow.call_count == 3


def test_insert_retry_all_fail() -> None:
    """CH stays unreachable: exception raised after 3 attempts."""
    ch_client = MagicMock()
    arrow_table = _make_arrow_table()

    ch_client.insert_arrow.side_effect = _ch_transport_error()

    with patch(_RETRY_SLEEP):
        with pytest.raises(ch_exc.OperationalError, match="Connection refused"):
            _insert_with_retry(ch_client=ch_client, table="traffic_event", arrow_table=arrow_table, deduplication_token="insert-1")

    assert ch_client.insert_arrow.call_count == 3


def test_fallback_all_succeed() -> None:
    """Large batch rejected; sub-chunks all succeed. DLQ never written."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()
    arrow_table = _make_arrow_table(4)

    def insert_side_effect(table, arrow_table, settings):
        if len(arrow_table) == 4:
            raise _ch_data_error()

    ch_client.insert_arrow.side_effect = insert_side_effect

    ch_producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event", dlq_producer=dlq_producer, split_factor=2)

    with patch(_RETRY_SLEEP) as sleep:
        ch_producer.produce_arrow(arrow_table)

    # The whole batch twice — once from produce_arrow, once at the top of the
    # descent — then the two halves.
    assert ch_client.insert_arrow.call_count == 4
    sleep.assert_not_called()
    dlq_producer.produce_arrow.assert_not_called()
    dlq_producer.flush.assert_called_once()


def test_dlq_single_bad_row() -> None:
    """A single row CH always rejects → written to DLQ once, without retries."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()

    bad_row = _make_arrow_table(1)
    ch_client.insert_arrow.side_effect = _ch_data_error()

    with patch(_RETRY_SLEEP) as sleep:
        _insert_with_dlq_fallback(
            table=bad_row,
            ch_client=ch_client,
            ch_table="traffic_event",
            dlq_producer=dlq_producer,
            split_factor=10,
            deduplication_token="insert-1",
        )

    assert ch_client.insert_arrow.call_count == 1
    sleep.assert_not_called()
    dlq_producer.produce_arrow.assert_called_once()
    sent = dlq_producer.produce_arrow.call_args[0][0]
    assert len(sent) == 1


def test_data_error_without_dlq_raises() -> None:
    """A bad row with no DLQ configured has nowhere to go, so it fails the batch."""
    ch_client = MagicMock()
    ch_client.insert_arrow.side_effect = _ch_data_error()

    ch_producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event", dlq_producer=None)

    with pytest.raises(ch_exc.DatabaseError, match="Cannot parse uuid"):
        ch_producer.produce_arrow(_make_arrow_table(2))


def test_recursive_descent() -> None:
    """4-row batch rejected; with split_factor=2, recursion finds and DLQs exactly
    the one bad row, and the three good rows still reach ClickHouse."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()

    def insert_side_effect(table, arrow_table, settings):
        if "uid-2" in arrow_table.column("uid").to_pylist():
            raise _ch_data_error()

    ch_client.insert_arrow.side_effect = insert_side_effect

    arrow_table = _make_arrow_table(4)  # uid-0, uid-1, uid-2, uid-3

    with patch(_RETRY_SLEEP):
        _insert_with_dlq_fallback(
            table=arrow_table,
            ch_client=ch_client,
            ch_table="traffic_event",
            dlq_producer=dlq_producer,
            split_factor=2,
            deduplication_token="insert-1",
        )

    dlq_producer.produce_arrow.assert_called_once()
    sent = dlq_producer.produce_arrow.call_args[0][0]
    assert len(sent) == 1
    assert sent.column("uid")[0].as_py() == "uid-2"

    inserted = set()
    for call in ch_client.insert_arrow.call_args_list:
        uids = call.kwargs["arrow_table"].column("uid").to_pylist()
        if "uid-2" not in uids:
            inserted.update(uids)
    assert inserted == {"uid-0", "uid-1", "uid-3"}


def test_data_error_splits_without_sleeping() -> None:
    """Regression test for audit F3: isolating a bad row used to cost 2s of sleep
    at every level of the descent. It must now cost round-trips only."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()

    def insert_side_effect(table, arrow_table, settings):
        if "uid-7" in arrow_table.column("uid").to_pylist():
            raise _ch_data_error()

    ch_client.insert_arrow.side_effect = insert_side_effect

    producer = ClickhouseProducer(
        ch_client=ch_client, table="traffic_event", dlq_producer=dlq_producer, split_factor=10
    )

    with patch(_RETRY_SLEEP) as sleep:
        producer.produce_arrow(_make_arrow_table(100))

    sleep.assert_not_called()
    dlq_producer.produce_arrow.assert_called_once()
    assert dlq_producer.produce_arrow.call_args[0][0].column("uid")[0].as_py() == "uid-7"
    dlq_producer.flush.assert_called_once()


def test_connection_error_is_retried_then_fails_the_batch() -> None:
    """The F3 fix proper: an outage is every row's problem, so it is retried and
    then raised for the node to rewind. It must never reach the DLQ."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()
    ch_client.insert_arrow.side_effect = _ch_transport_error()

    producer = ClickhouseProducer(
        ch_client=ch_client, table="traffic_event", dlq_producer=dlq_producer, split_factor=2
    )

    with patch(_RETRY_SLEEP):
        with pytest.raises(ch_exc.OperationalError, match="Connection refused"):
            producer.produce_arrow(_make_arrow_table(100))

    assert ch_client.insert_arrow.call_count == 3
    dlq_producer.produce_arrow.assert_not_called()
    dlq_producer.flush.assert_not_called()


def test_error_without_a_code_fails_the_batch() -> None:
    """`code is None` is what makes an error non-data, independent of its class:
    this one is a `DatabaseError`, like a real data error."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()
    ch_client.insert_arrow.side_effect = ch_exc.DatabaseError(
        "The ClickHouse server returned an error (for url http://localhost:8123)", code=None
    )

    producer = ClickhouseProducer(
        ch_client=ch_client, table="traffic_event", dlq_producer=dlq_producer
    )

    with patch(_RETRY_SLEEP):
        with pytest.raises(ch_exc.DatabaseError):
            producer.produce_arrow(_make_arrow_table(100))

    assert ch_client.insert_arrow.call_count == 3
    dlq_producer.produce_arrow.assert_not_called()


def test_non_data_error_mid_descent_aborts_the_whole_descent() -> None:
    """CH goes away part-way through isolating bad rows. The descent unwinds
    rather than filing the rest of the batch as rejected. Rows already sent to
    the DLQ stay sent, and are sent again once the rewound batch is re-read —
    the DLQ is at-least-once, like the output."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()

    def insert_side_effect(table, arrow_table, settings):
        uids = arrow_table.column("uid").to_pylist()
        if uids == ["uid-1"]:
            raise _ch_transport_error()
        raise _ch_data_error()

    ch_client.insert_arrow.side_effect = insert_side_effect

    producer = ClickhouseProducer(
        ch_client=ch_client, table="traffic_event", dlq_producer=dlq_producer, split_factor=2
    )

    with patch(_RETRY_SLEEP):
        with pytest.raises(ch_exc.OperationalError, match="Connection refused"):
            producer.produce_arrow(_make_arrow_table(4))

    # uid-0 was isolated and filed before CH went away; uid-2 and uid-3 were
    # never reached as singles.
    assert dlq_producer.produce_arrow.call_count == 1
    assert dlq_producer.produce_arrow.call_args[0][0].column("uid")[0].as_py() == "uid-0"
    dlq_producer.flush.assert_not_called()

    # uid-1 still gets its 3 attempts at that node: a blip mid-descent may pass,
    # and recovering there is worth more than discarding the isolation done so far.
    singles = [
        call.kwargs["arrow_table"].column("uid").to_pylist()[0]
        for call in ch_client.insert_arrow.call_args_list
        if len(call.kwargs["arrow_table"]) == 1
    ]
    assert singles == ["uid-0", "uid-1", "uid-1", "uid-1"]


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
    """produce_arrow hits a bad row: recursive fallback runs and DLQ is flushed."""
    ch_client = MagicMock()
    dlq_producer = MagicMock()
    arrow_table = _make_arrow_table(1)
    ch_client.insert_arrow.side_effect = _ch_data_error()

    producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event", dlq_producer=dlq_producer)

    producer.produce_arrow(arrow_table)

    dlq_producer.produce_arrow.assert_called_once()
    dlq_producer.flush.assert_called_once()


def test_ch_producer_failure_without_dlq_raises() -> None:
    """produce_arrow fails with no DLQ configured: exception propagates."""
    ch_client = MagicMock()
    arrow_table = _make_arrow_table(1)
    ch_client.insert_arrow.side_effect = _ch_transport_error()

    producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event", dlq_producer=None)

    with patch(_RETRY_SLEEP), pytest.raises(ch_exc.OperationalError, match="Connection refused"):
        producer.produce_arrow(arrow_table)


def test_ch_producer_as_dlq_for_another_ch_producer() -> None:
    """A ClickhouseProducer can itself be used as the dlq_producer for another ClickhouseProducer."""
    primary_ch_client = MagicMock()
    dlq_ch_client = MagicMock()
    arrow_table = _make_arrow_table(1)

    primary_ch_client.insert_arrow.side_effect = _ch_data_error()

    dlq_producer = ClickhouseProducer(ch_client=dlq_ch_client, table="traffic_event_dlq")
    producer = ClickhouseProducer(
        ch_client=primary_ch_client, table="traffic_event", dlq_producer=dlq_producer
    )

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


def test_ch_producer_records_the_whole_insert_as_deliver() -> None:
    """clickhouse_connect serializes and sends in one call, so there is no seam
    for serialize/enqueue — all of it lands in deliver, and flush adds nothing."""
    ch_client = MagicMock()
    stats = LoopStats(phases=PRODUCER_PHASES)

    producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event")
    producer.produce_arrow(_make_arrow_table(3), stats=stats)
    producer.flush(stats=stats)

    assert set(stats.phase_sec) == {"producer/deliver"}


def test_ch_producer_produce_pylist_records_serialize_and_deliver() -> None:
    ch_client = MagicMock()
    stats = LoopStats(phases=PRODUCER_PHASES)

    producer = ClickhouseProducer(ch_client=ch_client, table="traffic_event")
    producer.produce_pylist([{"uid": "uid-0", "traffic_in": 100}], stats=stats)

    assert set(stats.phase_sec) == {"producer/serialize", "producer/deliver"}


def test_ch_producer_does_not_pass_stats_to_its_dlq() -> None:
    """The DLQ fallback runs inside the primary's deliver block. Handing the
    DLQ the same stats would count that time twice."""
    ch_client = MagicMock()
    ch_client.insert_arrow.side_effect = _ch_data_error()
    dlq_producer = MagicMock()
    stats = LoopStats(phases=PRODUCER_PHASES)

    producer = ClickhouseProducer(
        ch_client=ch_client, table="traffic_event", dlq_producer=dlq_producer
    )
    producer.produce_arrow(_make_arrow_table(1), stats=stats)

    assert "stats" not in dlq_producer.produce_arrow.call_args.kwargs
    assert "stats" not in dlq_producer.flush.call_args.kwargs
    assert set(stats.phase_sec) == {"producer/deliver"}
