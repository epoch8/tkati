import datetime as dt
import tomllib
from collections.abc import Generator

import pyarrow as pa
import pytest
from loguru import logger
from tkati_core.testing import memory_pipelined_node
from tkati_node_el.main import TRANSFORM_PHASES, run
from tkati_node_el.settings import AppSettings, TransformSettings
from tkati_node_el.transform import Transform, load_transform


def _load(code: str) -> Transform:
    return load_transform(TransformSettings(code=code))


def test_load_returns_the_defined_transform() -> None:
    transform = _load(
        "import pyarrow as pa\n"
        "SUFFIX = '!'\n"
        "def transform(batch):\n"
        "    return batch.drop_columns(['b'])\n"
    )
    out = transform(pa.table({"a": [1], "b": [2]}))
    assert out.column_names == ["a"]


def test_dataclasses_work_inside_the_code() -> None:
    """The module is registered in `sys.modules`, which `dataclasses` needs."""
    transform = _load(
        "import dataclasses\n"
        "@dataclasses.dataclass\n"
        "class C:\n"
        "    x: int = 1\n"
        "def transform(batch):\n"
        "    return batch.append_column('x', [[C().x] * len(batch)])\n"
    )
    assert transform(pa.table({"a": [1, 2]})).column("x").to_pylist() == [1, 1]


def test_syntax_error_names_the_setting() -> None:
    with pytest.raises(SyntaxError) as info:
        _load("def transform(batch)\n    return batch\n")
    assert info.value.filename == "<settings: transform.code>"
    assert info.value.lineno == 1


def test_top_level_error_names_the_setting() -> None:
    with pytest.raises(ZeroDivisionError) as info:
        _load("x = 1\ny = 1 / 0\n")
    frame = info.traceback[-1]
    assert str(frame.path) == "<settings: transform.code>"
    assert frame.lineno + 1 == 2  # pytest's line numbers are 0-based


@pytest.mark.parametrize("code", ["x = 1\n", "transform = 1\n"])
def test_missing_or_non_callable_transform(code: str) -> None:
    with pytest.raises(TypeError, match="must define a function `transform"):
        _load(code)


def test_non_table_return_is_a_type_error() -> None:
    transform = _load("def transform(batch):\n    return batch.to_pylist()\n")
    with pytest.raises(TypeError, match=r"must return a pyarrow\.Table, got list"):
        transform(pa.table({"a": [1]}))


@pytest.fixture
def log_messages() -> Generator[list[str]]:
    messages: list[str] = []
    handler_id = logger.add(lambda m: messages.append(str(m)), level="INFO")
    yield messages
    logger.remove(handler_id)


def test_schema_change_is_warned(log_messages: list[str]) -> None:
    transform = _load(
        "import pyarrow as pa\n"
        "def transform(batch):\n"
        "    return pa.table({'v': pa.array([None] * len(batch))})\n"
    )
    transform(pa.table({"a": [1]}))
    transform(pa.table({"a": [1]}))
    assert sum("transform output schema:" in m for m in log_messages) == 1
    assert not any("changed" in m for m in log_messages)

    flipping = _load(
        "import pyarrow as pa\n"
        "def transform(batch):\n"
        "    return pa.table({'v': pa.array(batch.column('a').to_pylist())})\n"
    )
    flipping(pa.table({"a": [1]}))
    flipping(pa.table({"a": [None]}))
    assert any("schema changed" in m for m in log_messages)


def test_run_sends_the_transformed_table() -> None:
    transform = _load(
        "import polars as pl\n"
        "def transform(batch):\n"
        "    return pl.from_arrow(batch).with_columns(n=pl.col('n') * 2).to_arrow()\n"
    )
    node, consumer, producer = memory_pipelined_node(
        [pa.table({"n": [1, 2]}), pa.table({"n": [3]})], phases=TRANSFORM_PHASES
    )
    with node:
        run(node, transform)

    assert producer is not None
    assert [pa.table(t).column("n").to_pylist() for t in producer.sent] == [[2, 4], [6]]
    assert consumer.commits == [0, 1]
    assert node.stats.totals().phase_sec.keys() >= {"transform"}


def test_filtered_rows_count_as_dropped() -> None:
    transform = _load(
        "import pyarrow.compute as pc\n"
        "def transform(batch):\n"
        "    return batch.filter(pc.greater(batch['n'], 1))\n"
    )
    node, consumer, producer = memory_pipelined_node(
        [pa.table({"n": [1, 2, 3]}), pa.table({"n": [0]})], phases=TRANSFORM_PHASES
    )
    with node:
        run(node, transform)

    totals = node.stats.totals()
    assert (totals.rows_in, totals.rows_out) == (4, 2)
    # The all-filtered batch sends nothing, and is still committed.
    assert producer is not None
    assert [len(t) for t in producer.sent] == [2]
    assert consumer.commits == [0, 1]


def test_raising_transform_rewinds_its_batch() -> None:
    transform = _load(
        "def transform(batch):\n"
        "    if batch['n'][0].as_py() == 2:\n"
        "        raise RuntimeError('bad batch')\n"
        "    return batch\n"
    )
    node, consumer, _ = memory_pipelined_node(
        [pa.table({"n": [1]}), pa.table({"n": [2]}), pa.table({"n": [3]})],
        phases=TRANSFORM_PHASES,
    )
    with pytest.raises(RuntimeError, match="bad batch"):
        with node:
            run(node, transform)

    assert consumer.commits == [0]
    assert consumer.rewinds == [1]


README_SETTINGS = """
[input]
type = "kafka"

[input.connection]
broker = "redpanda:29092"

[input.topic]
name = "traffic_event"

[input.topic.schema]
time        = "string"        # producers write ISO-8601; read as text
package_id  = "string"        # producers write "7", not 7
country     = "string"
traffic_in  = "uint32"
traffic_out = "uint32"
local_ip    = "string"

[input.consumer]
group_id = "node-el-group"

[output]
type = "kafka"

[output.connection]
broker = "redpanda:29092"

[output.topic]
name = "traffic_event_landed"

[transform]
code = '''
import datetime as dt

import polars as pl
import pyarrow as pa

DEFAULT_COUNTRY = "ZZ"


def transform(batch: pa.Table) -> pa.Table:
    df = pl.from_arrow(batch)
    now = dt.datetime.now(dt.UTC)
    df = (
        df.with_columns(
            # Type adaptation. strict=False turns junk into null instead of
            # failing the batch; leave it strict where junk should stop the node.
            pl.col("time").str.to_datetime(time_unit="ms"),
            pl.col("package_id").cast(pl.Int32, strict=False),
            # Defaults: a missing field arrives as null.
            pl.col("country").fill_null(DEFAULT_COUNTRY),
            # Computed columns. Widen first: UInt32 + UInt32 wraps silently.
            traffic_total=pl.col("traffic_in").cast(pl.UInt64)
            + pl.col("traffic_out").cast(pl.UInt64),
            ingested_at=pl.lit(now, dtype=pl.Datetime("ms", "UTC")),
        )
        .with_columns(event_date=pl.col("time").dt.date())
        # Filter rows, drop columns the target doesn't have.
        .filter(pl.col("traffic_total") > 0)
        .drop("local_ip")
    )
    return df.to_arrow()
'''
"""


def test_readme_example() -> None:
    settings = AppSettings(**tomllib.loads(README_SETTINGS))
    assert settings.transform is not None
    transform = load_transform(settings.transform)

    batch = pa.table(
        {
            "time": [
                "2026-10-05T12:00:00.123",
                "2026-10-05T12:00:01",
                "2026-10-05T12:00:02",
            ],
            "package_id": ["7", "8", "x"],
            "country": ["RU", None, "DE"],
            "traffic_in": pa.array([4294967295, 0, 5], pa.uint32()),
            "traffic_out": pa.array([1, 0, 1], pa.uint32()),
            "local_ip": ["10.0.0.1", "10.0.0.2", "10.0.0.3"],
        }
    )
    out = transform(batch)

    assert out.column_names == [
        "time",
        "package_id",
        "country",
        "traffic_in",
        "traffic_out",
        "traffic_total",
        "ingested_at",
        "event_date",
    ]
    rows = out.to_pylist()
    assert [r["package_id"] for r in rows] == [7, None]
    assert [r["country"] for r in rows] == ["RU", "DE"]
    assert [r["traffic_total"] for r in rows] == [4294967296, 6]
    assert rows[0]["time"] == dt.datetime(2026, 10, 5, 12, 0, 0, 123000)
    assert rows[0]["event_date"] == dt.date(2026, 10, 5)
    assert out.schema.field("ingested_at").type == pa.timestamp("ms", tz="UTC")


def test_readme_default_country() -> None:
    """The row whose country was missing was filtered above; check the default
    on its own."""
    settings = AppSettings(**tomllib.loads(README_SETTINGS))
    assert settings.transform is not None
    out = load_transform(settings.transform)(
        pa.table(
            {
                "time": ["2026-10-05T12:00:00"],
                "package_id": ["1"],
                "country": pa.array([None], pa.string()),
                "traffic_in": pa.array([1], pa.uint32()),
                "traffic_out": pa.array([1], pa.uint32()),
                "local_ip": ["10.0.0.1"],
            }
        )
    )
    assert out.column("country").to_pylist() == ["ZZ"]
