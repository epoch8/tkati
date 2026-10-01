from datetime import UTC, datetime

import clickhouse_connect as ch
import clickhouse_connect.driver as ch_driver
import clickhouse_connect.driver.exceptions as ch_exc
import orjson
import pyarrow as pa
from loguru import logger
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_fixed,
)

from tkati_core.clickhouse.settings import ClickHouseOutputSettings
from tkati_core.producer import Producer
from tkati_core.stats import PhaseStats

# ClickHouse server error codes that mean the *rows* are bad, not the server or
# the connection: parse and value errors. Names verified against
# `errorCodeToName` in `clickhouse local`. Only these send an insert down the
# recursive split to the DLQ, and only these skip the retries — no wait fixes a
# bad row. Anything else is retried and then raised, so the node rewinds its
# batch instead of emptying it into the DLQ.
#
# Rebind this set at startup to change the policy; there is no setting for it.
#
# Schema-shape errors are deliberately absent: TYPE_MISMATCH (53),
# NO_SUCH_COLUMN_IN_TABLE (16), THERE_IS_NO_COLUMN (8),
# SIZES_OF_COLUMNS_DOESNT_MATCH (9), NUMBER_OF_COLUMNS_DOESNT_MATCH (20), and
# 43, 44, 50, 70. A missed migration rejects every row alike, so treating it as
# a data error would drain whole batches into the DLQ. It should stop the node.
CH_DATA_ERROR_CODES: frozenset[int] = frozenset(
    {
        6,  # CANNOT_PARSE_TEXT
        25,  # CANNOT_PARSE_ESCAPE_SEQUENCE
        26,  # CANNOT_PARSE_QUOTED_STRING
        27,  # CANNOT_PARSE_INPUT_ASSERTION_FAILED
        38,  # CANNOT_PARSE_DATE
        41,  # CANNOT_PARSE_DATETIME
        72,  # CANNOT_PARSE_NUMBER
        117,  # INCORRECT_DATA
        128,  # TOO_LARGE_ARRAY_SIZE
        131,  # TOO_LARGE_STRING_SIZE
        321,  # VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE
        349,  # CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN
        376,  # CANNOT_PARSE_UUID
        407,  # DECIMAL_OVERFLOW
        434,  # CANNOT_PARSE_PROTOBUF_SCHEMA
        441,  # CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING
        467,  # CANNOT_PARSE_BOOL
        632,  # UNEXPECTED_DATA_AFTER_PARSED_VALUE
        675,  # CANNOT_PARSE_IPV4
        676,  # CANNOT_PARSE_IPV6
    }
)

# Attempts for an insert that failed for a reason other than the data.
_INSERT_ATTEMPTS = 3


def _is_data_error(err: BaseException) -> bool:
    """True when ClickHouse rejected the rows themselves.

    Keyed on the server error code, not on the exception class:
    `HttpClient._error_handler` picks `DatabaseError` or `OperationalError` by
    whether the request was retried, not by what went wrong, and sets `code`
    from the `X-ClickHouse-Exception-Code` header either way. A pure transport
    failure raises `OperationalError` with `code=None`. `name` is only populated
    when `show_clickhouse_errors` is on, so it is good for log lines and nothing
    else.

    Note this is unrelated to the driver's own DB-API `DataError` class, which
    can be raised client-side with no code and so counts as *not* a data error
    here.
    """
    return isinstance(err, ch_exc.Error) and err.code in CH_DATA_ERROR_CODES


def _describe(err: BaseException) -> str:
    """`err` for a log line, with the ClickHouse error code, which `str(err)`
    leaves out when the server's error detail is suppressed. The code is what
    tells an operator whether it belongs in `CH_DATA_ERROR_CODES`."""
    if isinstance(err, ch_exc.Error) and err.code is not None:
        return f"{err} [code {err.code} {err.name or '?'}]"
    return str(err)


def log_retry_attempt(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome is not None else None
    logger.warning(
        f"Retrying {retry_state.fn} after {exc}, "
        f"attempt {retry_state.attempt_number}/{_INSERT_ATTEMPTS}"
    )


@retry(
    # Data errors are not retried at all: no wait fixes a bad row, and those
    # sleeps are what made a recursive descent take half an hour (audit F3).
    # tenacity re-raises immediately when the predicate says no.
    retry=retry_if_exception(lambda err: not _is_data_error(err)),
    stop=stop_after_attempt(_INSERT_ATTEMPTS),
    wait=wait_fixed(1),
    before_sleep=log_retry_attempt,
    reraise=True,
)
def _insert_with_retry(
    ch_client: ch_driver.Client, table: str, arrow_table: pa.Table
) -> None:
    """Insert, retrying anything that isn't a data error (see `_is_data_error`)."""
    ch_client.insert_arrow(table=table, arrow_table=arrow_table)


def _table_slices(table: pa.Table, chunk_size: int) -> list[pa.Table]:
    return [table.slice(i, chunk_size) for i in range(0, len(table), chunk_size)]


def _make_dlq_table(
    ch_client: ch_driver.Client,
    ch_table: str,
    row_table: pa.Table,
    err: Exception,
) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "producer": orjson.dumps(
                    {
                        "ch_url": ch_client.uri,
                        "ch_database": ch_client.database,
                        "ch_table": ch_table,
                    }
                ).decode(),
                "data": orjson.dumps(row_table.to_pylist()[0]).decode(),
                "err_message": str(err),
                "time": datetime.now(UTC),
            }
        ]
    )


def _insert_with_dlq_fallback(
    table: pa.Table,
    ch_client: ch_driver.Client,
    ch_table: str,
    dlq_producer: Producer,
    split_factor: int,
) -> None:
    try:
        _insert_with_retry(ch_client=ch_client, table=ch_table, arrow_table=table)
        return
    except Exception as err:
        if not _is_data_error(err):
            # Not the rows' fault: abort the descent and let the caller fail the
            # batch, rather than filing good rows in the DLQ as rejected. Raising
            # from inside this `except` skips every parent frame's handler, so
            # this unwinds the whole descent and logs exactly once.
            logger.error(
                f"Insert of {len(table)} rows failed for a reason other than the "
                f"data ({_describe(err)}), failing the batch"
            )
            raise
        if len(table) == 1:
            logger.error(
                f"Single row rejected by ClickHouse ({_describe(err)}), sending to DLQ"
            )
            dlq_producer.produce_arrow(
                _make_dlq_table(
                    ch_client,
                    ch_table,
                    table,
                    err,
                )
            )
            return
        chunk_size = max(1, len(table) // split_factor)
        logger.warning(
            f"Chunk of {len(table)} rows rejected ({_describe(err)}), "
            f"splitting into chunks of {chunk_size} rows"
        )
        for chunk in _table_slices(table, chunk_size):
            _insert_with_dlq_fallback(
                chunk, ch_client, ch_table, dlq_producer, split_factor
            )


class ClickhouseProducer(Producer):
    def __init__(
        self,
        ch_client: ch_driver.Client,
        table: str,
        dlq_producer: Producer | None = None,
        split_factor: int = 10,
    ) -> None:
        self._ch_client = ch_client
        self._table = table
        self._dlq_producer = dlq_producer
        self._split_factor = split_factor

    @classmethod
    def from_output_settings(
        cls,
        settings: ClickHouseOutputSettings,
        dlq_producer: Producer | None = None,
    ) -> "ClickhouseProducer":
        ch_client = ch.get_client(
            host=settings.connection.host,
            port=settings.connection.port,
            username=settings.connection.user,
            password=settings.connection.password,
            database=settings.table.database,
            secure=settings.connection.secure,
        )
        return cls(
            ch_client=ch_client,
            table=settings.table.name,
            dlq_producer=dlq_producer,
            split_factor=settings.dlq_split_factor,
        )

    def produce_arrow(
        self,
        data: pa.Table,
        stats: PhaseStats | None = None,
        tag: int | None = None,
    ) -> None:
        # All of it is `deliver`: clickhouse_connect serializes and sends inside
        # one call, so there is no seam to put `serialize`/`enqueue` on. That
        # includes the retry and DLQ fallback, whose own producer is deliberately
        # not handed `stats` — its time is already inside this block, and
        # recording it again would count it twice. The split doesn't sleep (data
        # errors aren't retried), so isolating bad rows costs round-trips here,
        # not minutes.
        stats = stats if stats is not None else PhaseStats(phases=())

        with stats.phase("producer/deliver"):
            try:
                _insert_with_retry(
                    ch_client=self._ch_client, table=self._table, arrow_table=data
                )
            except Exception as err:
                if not _is_data_error(err) or self._dlq_producer is None:
                    raise
                logger.warning(
                    f"Batch of {len(data)} rows rejected by ClickHouse "
                    f"({_describe(err)}), isolating the bad rows with "
                    f"split_factor={self._split_factor}"
                )
                _insert_with_dlq_fallback(
                    table=data,
                    ch_client=self._ch_client,
                    ch_table=self._table,
                    dlq_producer=self._dlq_producer,
                    split_factor=self._split_factor,
                )
                self._dlq_producer.flush()

    def produce_pylist(
        self,
        rows: list[dict],
        stats: PhaseStats | None = None,
        tag: int | None = None,
    ) -> None:
        stats = stats if stats is not None else PhaseStats(phases=())
        with stats.phase("producer/serialize"):
            table = pa.Table.from_pylist(rows)
        self.produce_arrow(table, stats=stats)

    def wait_delivered(self, tag: int, timeout: float | None = None) -> bool:
        """Always True: an insert has either succeeded or raised by the time
        `produce_arrow` returns, so there is nothing to wait for. `tag` is
        accepted by the produce methods only to match `Producer`."""
        return True

    def flush(self, stats: PhaseStats | None = None) -> None:
        """No-op: ClickHouse inserts are synchronous, nothing to flush. Their
        wait is recorded as `producer/deliver` by produce_arrow instead."""

    def close(self) -> None:
        self._ch_client.close()
