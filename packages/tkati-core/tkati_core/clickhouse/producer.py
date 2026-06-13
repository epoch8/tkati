import clickhouse_connect as ch
import clickhouse_connect.driver as ch_driver
import pyarrow as pa
from loguru import logger
from tenacity import RetryCallState, retry, stop_after_attempt, wait_fixed
from tkati_core.producer import KafkaProducer

from tkati_core.clickhouse.settings import ClickHouseOutputSettings


def log_retry_attempt(retry_state: RetryCallState) -> None:
    fn_name = retry_state.fn.__name__ if retry_state.fn is not None else "unknown"
    exc = retry_state.outcome.exception() if retry_state.outcome is not None else None
    logger.warning(
        f"Retrying {fn_name} after {exc}, "
        f"attempt {retry_state.attempt_number}/3"
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    before_sleep=log_retry_attempt,
    reraise=True,
)
def _insert_with_retry(ch_client: ch_driver.Client, table: str, arrow_table: pa.Table) -> None:
    ch_client.insert_arrow(table=table, arrow_table=arrow_table)


def _table_slices(table: pa.Table, chunk_size: int) -> list[pa.Table]:
    return [table.slice(i, chunk_size) for i in range(0, len(table), chunk_size)]


def _insert_with_dlq_fallback(
    table: pa.Table,
    ch_client: ch_driver.Client,
    ch_table: str,
    dlq_producer: KafkaProducer,
    split_factor: int,
) -> None:
    try:
        _insert_with_retry(ch_client=ch_client, table=ch_table, arrow_table=table)
        return
    except Exception as err:
        if len(table) == 1:
            logger.error(f"Single row rejected by ClickHouse ({err}), sending to DLQ")
            dlq_producer.produce_arrow(table)
            return
        chunk_size = max(1, len(table) // split_factor)
        logger.warning(
            f"Chunk of {len(table)} rows failed ({err}), "
            f"splitting into chunks of {chunk_size} rows"
        )
        for chunk in _table_slices(table, chunk_size):
            _insert_with_dlq_fallback(chunk, ch_client, ch_table, dlq_producer, split_factor)


class ClickhouseProducer:
    def __init__(
        self,
        ch_client: ch_driver.Client,
        table: str,
        dlq_producer: KafkaProducer | None = None,
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
        dlq_producer: KafkaProducer | None = None,
        split_factor: int = 10,
    ) -> "ClickhouseProducer":
        ch_client = ch.get_client(
            host=settings.host,
            port=settings.port,
            username=settings.user,
            password=settings.password,
            database=settings.database,
            secure=settings.secure,
        )
        return cls(ch_client=ch_client, table=settings.table, dlq_producer=dlq_producer, split_factor=split_factor)

    def produce_arrow(self, arrow_table: pa.Table) -> None:
        try:
            _insert_with_retry(ch_client=self._ch_client, table=self._table, arrow_table=arrow_table)
        except Exception as err:
            if self._dlq_producer is None:
                raise
            logger.warning(
                f"Batch of {len(arrow_table)} rows failed ({err}), "
                f"switching to recursive fallback with split_factor={self._split_factor}"
            )
            _insert_with_dlq_fallback(
                table=arrow_table,
                ch_client=self._ch_client,
                ch_table=self._table,
                dlq_producer=self._dlq_producer,
                split_factor=self._split_factor,
            )
            self._dlq_producer.flush()
