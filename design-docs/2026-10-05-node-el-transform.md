---
status: IMPLEMENTED
---

# tkati-node-el: a `[transform]` section running Python over each Arrow batch

## Context

`tkati-node-el` moves rows from its input to its output and changes nothing on
the way. Its whole loop body is one line
(`packages/tkati-node-el/src/tkati_node_el/main.py`):

```python
for event in node.consume_arrow():
    if isinstance(event, Batch):
        node.done(event, output_arrow=event.data)
```

`AppSettings` (`packages/tkati-node-el/src/tkati_node_el/settings.py`) adds
nothing to `NodeSettings` beyond making `output` required. So whatever reaches
the output is exactly what the input decoder produced: the columns named in
`[input.topic.schema]`, typed through `TYPE_MAPPING`
(`packages/tkati-core/tkati_core/type_mapping.py`), and nothing else.

That is now too little for the streams we land. The cases that keep coming up:

- **Type adaptation.** A producer writes a number as a JSON string, or a
  timestamp as ISO-8601. `TYPE_MAPPING` fixes one wire spelling per type, so
  such a field can only be read as `string`
  (`design-docs/2026-10-schema-definition-language/01-Current-State.md` has the
  probe) and lands in ClickHouse as a string, or fails the insert. What is
  needed is "read it as a string, cast it to what the table wants".
- **Default values.** `read_json` with an explicit schema turns a missing field
  into null. Some target columns are not `Nullable`, and the right default is a
  business decision (`country = "ZZ"`), not ClickHouse's zero value.
- **Additional fields.** Columns computed from the row — `traffic_in +
  traffic_out`, a `Date` partition column from `time`, an `ingested_at`
  stamp — that today have to be ClickHouse `DEFAULT`/`MATERIALIZED` expressions
  (ClickHouse output only, logic hidden in DDL) or another service in front of
  the topic.
- **Dropping things.** Columns the target doesn't have, and rows that shouldn't
  be landed at all.

All of these are batch-in, batch-out functions over an Arrow table, and the
harness already allows exactly that: `Batch` (`packages/tkati-core/tkati_core/node.py`)
keeps the batch as read for commit and rewind, and "the node only sees its
data, so filtering or transforming `data` never changes what is committed".
`done(event, output_arrow=...)` takes any table, counts its rows as rows out,
and sends nothing for an empty one. The seam exists; there is no way to put
code in it without forking the node.

The schema catalog design
(`design-docs/2026-10-schema-definition-language/`) explicitly leaves
transformations out ("A schema describes a stream; it does not reshape one")
and only covers sink adaptations a sink *forces*. This doc is the other half:
reshaping a stream on purpose, in the node that lands it.

## Goal

Done when:

- A `tkati-node-el` deployment can apply a Python function
  `transform(batch: pa.Table) -> pa.Table` to every batch between reading and
  writing, written inline in `settings.toml`, without building a new node.
- A broken transform — syntax error, no `transform` defined, `transform` not
  callable — fails at startup, before the node connects to anything, with a
  message naming the setting.
- A transform that raises on a batch fails that batch the way any other
  non-data error does: rewound, not committed, not sent to the DLQ.
- The time the transform takes is visible as its own phase on the `perf loop:`
  line and in `/metrics`, and rows it filters show up as `dropped`.
- The README shows the three motivating cases (type adaptation, defaults,
  computed columns) as a working example.
- A deployment without `[transform]` behaves and logs exactly as today.

Non-goals:

- **Code from anywhere but `settings.toml`.** No `.py` file reference, no
  importable module, no factory with parameters. One source keeps the loader,
  the validation and the docs to one case; see *Alternatives* and *Later*.
- **A declarative transform language** (per-column `default =`, `cast =` in
  TOML) or SQL. See *Alternatives*.
- **Row-level rejection to the DLQ.** A transform either returns a table or
  fails the batch. Routing individual bad rows to the DLQ is a later extension
  (*Later*), and shares its problem with audit F1.
- **Sandboxing.** The code runs in the node's process with the node's
  privileges. Settings are already trusted input (they hold credentials).
- **Transforms in other nodes.** `tkati-node-dedup` could use the same hook;
  moving the loader into `tkati-core` is a follow-up once one node has proven
  the shape.
- **Declaring or enforcing the output schema.** That belongs to the schema
  catalog's IR; until it exists the transform's output is whatever it returns.
- **Row-at-a-time (`list[dict]`) transforms.** The node reads with
  `consume_arrow()`; a transform that wants dicts calls `to_pylist()` itself and
  pays for it visibly.

## Approach

Give `tkati-node-el` one optional `[transform]` section holding a piece of
Python source. The node executes it once at startup as a module, takes the
`transform` function it defines, and calls it on each batch's table right
before `done()`. Nothing in `tkati-core` changes.

The unit of work is the **Arrow table**, not the row, because that is what the
node already holds and what both outputs consume natively: ClickHouse via
`insert_arrow`, Kafka via the native encoder. A transform written with
Polars or `pyarrow.compute` stays vectorised end to end; a per-row Python
transform would cost more than the rest of the node combined at production
batch sizes.

**Polars ships with the node, and is the recommended way to write a
transform.** `tkati-node-el` depends on `polars`, so every deployment can
`import polars` in its transform without a custom image. Its expression API
covers the motivating cases more directly than `pyarrow.compute` — notably
`cast(..., strict=False)`, which turns an unparseable value into null instead
of failing the batch — and `pl.from_arrow` / `DataFrame.to_arrow()` convert
without copying the data. The contract itself stays `pa.Table` in and out:
that is what the harness and both producers speak, and a transform that only
needs `batch.drop_columns(...)` shouldn't pay for a conversion.

**Python, rather than a configuration language**, because the motivating
cases differ in every deployment and each keeps growing a special case (a cast
that needs a format, a default that depends on another column). A small TOML
vocabulary would chase that indefinitely, and every addition would be a
release of `tkati-node-el`.

**Inline code only, with a fixed name.** The transforms we need today are
short — a few casts, a few `fill_null`s, a computed column — and live with the
schema they adapt, which is already in `settings.toml`. Keeping them in the
same file means one artifact to deploy and review, and a fixed entry point
(`transform`) means nothing to configure but the code. Setup work that should
happen once (constants, a compiled regex, a lookup table) goes at the top
level of the code, which runs once at startup.

**A failing transform fails the batch.** An exception from the transform is
either a bug in the transform or a row it didn't anticipate; in both cases
quarantining the whole batch in the DLQ would be wrong for the same reason it
is wrong for a ClickHouse schema error
(`design-docs/2026-09-28-clickhouse-error-classification.md`): every row would
go, and the next batch would follow. Propagating out of the loop rewinds the
batch, the node exits and is restarted, and the operator sees the traceback.
The cost is the same stall F1 describes for a bad message; transforms should
therefore be written not to raise on data (see *Writing a transform*).

## Design

### Settings

```toml
[transform]
code = '''
import polars as pl
import pyarrow as pa

def transform(batch: pa.Table) -> pa.Table:
    return pl.from_arrow(batch).drop("local_ip").to_arrow()
'''
```

`TransformSettings`, in `tkati_node_el/settings.py`, has one required field,
`code: str`. `AppSettings` gains `transform: TransformSettings | None = None`.
A TOML multi-line literal string (`'''`) takes Python verbatim — no escaping of
backslashes or quotes, which matters for regexes and format strings.

Like every other section, it can be overridden from the environment
(`TRANSFORM__CODE=...`), via `TomlBaseSettings`' `env_nested_delimiter`.

### The contract

```python
def transform(batch: pa.Table) -> pa.Table: ...
```

- Called on the loop thread, once per non-empty batch, in read order, with the
  table `consume_arrow()` produced: the columns of `[input.topic.schema]`, with
  their internal types.
- Returns a `pa.Table`. Anything else is a `TypeError`.
- May return fewer rows (filter), zero rows (`done()` then sends nothing and
  still commits), or more rows (counted as negative `dropped`).
- May add, drop, rename, reorder and retype columns. For a ClickHouse output
  the returned column names are the insert's column list (`insert_arrow` takes
  them from the table), so omitted columns get the table's `DEFAULT`; for a
  Kafka output they are the JSON keys.
- Column types are whatever the returned table has. Polars' `to_arrow()`
  returns strings as `large_string`, not `string`; the native JSON encoder
  handles `LargeUtf8` (`packages/tkati-core/src/encode.rs`), and ClickHouse's
  Arrow input reads `LARGE_STRING` into `String`. The ClickHouse roundtrip test
  in *Implementation Steps* pins the latter.
- Should not keep references to batches across calls: with `PipelinedNode` the
  previous batch may still be in flight when the next is transformed. (Arrow
  tables are immutable, so this is about memory, not races.)
- Fields absent from `[input.topic.schema]` never reach the transform: the
  JSON reader is called with `unexpected_field_behavior="ignore"`. A computed
  column's inputs must be declared there.

### Loading

`tkati_node_el/transform.py`:

```python
type Transform = Callable[[pa.Table], pa.Table]

_FILENAME = "<settings: transform.code>"


def load_transform(settings: TransformSettings) -> Transform:
    module = types.ModuleType("tkati_transform")
    module.__file__ = _FILENAME
    sys.modules[module.__name__] = module
    exec(compile(settings.code, _FILENAME, "exec"), module.__dict__)
    fn = module.__dict__.get("transform")
    if not callable(fn):
        raise ValueError("transform.code must define a function `transform(batch)`")
    return _checked(fn)
```

- Compiling with a named filename makes tracebacks read
  `File "<settings: transform.code>", line 12`; line numbers count from the
  first line of the string. A `SyntaxError` at startup reports the same.
- Registering the module in `sys.modules` lets `dataclasses` and similar
  module-introspecting code inside it work.
- `_checked` wraps the function to enforce the return type and to log the
  output schema: at INFO on the first batch, at WARNING whenever it differs
  from the previous batch's. A schema that flips between batches is almost
  always a bug (e.g. `pa.array` inferring `null` for an all-`None` column) and
  will surface as a ClickHouse type error or inconsistent JSON; the warning
  names it first. It does not fail the batch: the schema is the transform's to
  decide until the catalog can say otherwise.

`main()` loads the transform **before** `PipelinedNode.from_settings`, so a
broken transform exits without opening a Kafka consumer or a ClickHouse
connection.

### The loop

```python
TRANSFORM_PHASES = ("wait/input", "transform", *PRODUCER_PHASES, "wait/in-flight", "commit")

def run(node: PipelinedNode, transform: Transform | None = None) -> None:
    for event in node.consume_arrow():
        if isinstance(event, Batch):
            output = event.data
            if transform is not None:
                with node.stats.phase("transform"):
                    output = transform(output)
            node.done(event, output_arrow=output)
```

`main()` passes `phases=TRANSFORM_PHASES` to `from_settings` only when a
transform is configured, so the perf line and metric labels of existing
deployments don't gain a column that always reads 0%. `transform` sits where
it runs: after the loop takes the batch, before the producer encodes it.

An exception from `transform` propagates out of `run()`; `_NodeBase.__exit__`
rewinds the uncommitted batch and the process exits non-zero. Batches already
handed to `done()` are drained and committed first, as on any failure.

### Perf, threading and stats

The transform runs on the loop thread, in parallel with the read-ahead thread
parsing the next batch and with earlier batches being delivered. On the
`perf loop:` line:

```
perf loop: wait/input=0.10s (1%) transform=1.40s (14%) producer/serialize=2.10s (21%) ...
```

High `transform` with low `wait/input` means the transform is the bottleneck.
Polars runs an expression on its own thread pool with the GIL released, as do
`pyarrow.compute` kernels; Python-level loops (`map_elements`, `to_pylist()`)
hold it and compete with the read-ahead thread's Python parts, so they show up
as both `transform` and slower `consumer/parse`.

Rows a transform filters out are counted by the existing `rows_in - rows_out`
as `dropped`; the node-el README's "this node never drops rows" becomes "only
a transform drops rows".

### Writing a transform

The README gets this example, covering the three motivating cases on the
`traffic_event` schema from `settings.test.toml`. The code runs as written
(checked against the workspace's pyarrow).

```toml
[input.topic.schema]
time        = "string"        # producers write ISO-8601; read as text
package_id  = "string"        # producers write "7", not 7
country     = "string"
traffic_in  = "uint32"
traffic_out = "uint32"
local_ip    = "string"
# ...

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
```

Run on three rows through the native Kafka encoder, it gives (`package_id
"x"` became null, the all-zero-traffic row was filtered, `4294967295 + 1` did
not wrap):

```json
{"time":"2026-10-05T12:00:00.123000","package_id":7,"country":"RU","traffic_in":4294967295,"traffic_out":1,"traffic_total":4294967296,"ingested_at":"2026-10-05T16:12:47.222000+00:00","event_date":"2026-10-05"}
{"time":"2026-10-05T12:00:02","package_id":null,"country":"DE","traffic_in":5,"traffic_out":1,"traffic_total":6,"ingested_at":"2026-10-05T16:12:47.222000+00:00","event_date":"2026-10-05"}
```

Guidance the README states alongside it:

- **Strict casts fail the whole batch.** Polars casts are strict by default:
  `pl.col("package_id").cast(pl.Int32)` on `"x"` raises, which stalls the node
  on that batch. Decide per field whether junk means "null or a default"
  (`strict=False`, then `fill_null`) or "stop" (strict). The same holds for
  `str.to_datetime`, which also infers the format from the data: pass
  `format=` when producers are known to mix spellings.
- **Overflow is silent.** Arithmetic on narrow integer types wraps; widen
  first, as `traffic_total` does above.
- **Stay columnar.** Polars expressions, not `map_elements` or a Python loop
  over rows; those work, and show up as the `transform` phase.
- **Top-level code runs once**, at startup: put constants and expensive setup
  there, not in `transform`.
- **Test it** by loading the settings: `load_transform(AppSettings().transform)`
  returns the same function the node calls, to run against a hand-built
  `pa.table({...})` in pytest, no broker needed.
- Libraries beyond Polars and pyarrow (DuckDB, a geo-IP reader) are the
  image's business: build `FROM` the node's image and install them.

## Alternatives

**Code from a file or an importable module, with a configurable function name
and a parametrised factory.** Considered in the first draft of this doc: a
`.py` file gets an editor, a linter and its own unit tests, and a factory lets
one transform serve several deployments. Each is another key, another
validation rule and another loading path to document and test, for needs we
don't have yet. Inline code covers today's cases; the others can be added
without changing the contract (*Later*).

**A Polars contract, `transform(df: pl.DataFrame) -> pl.DataFrame`.** Saves
the two conversion lines in every transform. But the harness, both producers
and the DLQ speak Arrow, so the node would convert on every batch whether the
transform needs Polars or not, and the contract would be tied to Polars' API
and its Arrow type choices. With `pa.Table` the conversion is visible in the
transform, and free in practice.

**Declarative columns in TOML** — `[transform.columns.country] default = "ZZ"`,
`cast = "int32"`, `expr = "traffic_in + traffic_out"`. Inspectable and safe,
but `expr` is a language we'd have to write and maintain, and each case that
doesn't fit is a release.

**SQL over the batch** (DuckDB or DataFusion, `SELECT *, traffic_in +
traffic_out AS traffic_total FROM batch`). Declarative, expressive, Arrow
native. Adds a large dependency to every deployment for a minority of them,
and a per-batch planning cost. A `transform` that calls DuckDB on the batch
gets the same thing for those who want it.

**A separate transform node between the topic and node-el.** Every
transformed stream would cost an extra topic, an extra produce and consume of
every row through the broker, and an extra deployment — for work that takes a
fraction of node-el's own time.

**ClickHouse `DEFAULT` / `MATERIALIZED` columns and materialised views.**
Works today, for ClickHouse outputs only, and stays a fine choice there for
pure derivations. It cannot help a Kafka output, can't fix a value the insert
already rejects (a string in an `Int32` column), and puts pipeline logic in DDL
that nobody reviews with the node.

## Later

- **Other sources.** A `file` key (a `.py` next to `settings.toml`, for
  transforms that outgrow a TOML string) and a `module` key (packaged in an
  image). Both would produce the same module namespace and look up the same
  `transform`, so the contract and the loop don't change.
- **Row rejection.** Let `transform` return `(output, rejected)`, sending
  `rejected` (with a reason column) to the DLQ producer the node already has.
  This would let "junk means DLQ" be a per-field choice instead of a stall, and
  is the transform-side answer to F1. Needs the DLQ producer exposed on the
  node and a decision on the rejected table's schema.
- **Move the loader to `tkati-core`** so `tkati-node-dedup` gets the same
  section.
- **Schema catalog integration.** Once a stream's IR exists, legacy spellings
  (ISO timestamps, numbers as strings) move out of transforms and into the
  catalog entry, and the transform's output can be checked against the target's
  IR at startup instead of warned about at runtime.

## Implementation Steps

0. **Dependency.** `polars>=1.44.2` in
   `packages/tkati-node-el/pyproject.toml`'s `dependencies` (done:
   `uv add --package tkati-node-el --no-sync polars`, then
   `uv sync --all-packages`; `uv.lock` is gitignored).
1. **Settings.** In `packages/tkati-node-el/src/tkati_node_el/settings.py` add
   `TransformSettings(BaseModel)` with `code: str`, and
   `transform: TransformSettings | None = None` on `AppSettings`.
2. **Loader.** New `packages/tkati-node-el/src/tkati_node_el/transform.py`:
   `type Transform = Callable[[pa.Table], pa.Table]`;
   `load_transform(settings: TransformSettings) -> Transform` as in *Loading*;
   private `_checked` (return-type check, first/changed output schema logging).
3. **Loop.** In `packages/tkati-node-el/src/tkati_node_el/main.py`: add
   `TRANSFORM_PHASES`; change `run(node)` to `run(node, transform=None)` timing
   the call under `node.stats.phase("transform")`; in `main()`, load the
   transform first, then
   `PipelinedNode.from_settings(settings, phases=TRANSFORM_PHASES if transform else None)`.
   The existing `run(node)` call sites in `tests/test_node_el.py` keep working.
4. **Tests.** New `packages/tkati-node-el/tests/test_transform.py`, broker-free:
   - loading: a valid `code`; a syntax error and a runtime error at top level
     both fail `load_transform`, the traceback naming
     `<settings: transform.code>`; no `transform` defined, and `transform = 1`,
     give the "must define" error;
   - a non-table return raises `TypeError`;
   - `run()` over `tkati_core.testing.memory_pipelined_node`: output is the
     transformed table; a filtering transform shows as `dropped`; a transform
     that raises leaves its batch uncommitted (rewound) and propagates;
   - the README example, loaded from a TOML string through `AppSettings`,
     against a hand-built table.
   Add one case to `test_node_el.py` running the ClickHouse roundtrip with a
   Polars transform that adds a column and returns `large_string` columns,
   against a table that has the column.
5. **Docs.** `packages/tkati-node-el/README.md`: a `## Transform` section
   (setting, contract, the example, the guidance list), the `transform` phase
   in *Metrics and perf log*, and the "never drops rows" sentence. Entry in
   `packages/tkati-node-el/CHANGELOG.md` and the root `CHANGELOG.md`.
6. **Release.** New optional section, no behaviour change without it: a minor
   bump (`0.9.0`) across the workspace per `AGENTS.md` *Versioning*, then
   `uv sync --all-packages`. Set this doc's `status` to `IMPLEMENTED` and add
   *Implementation notes (as built)*.
7. **Checks.** `uv run ruff check packages/tkati-node-el`,
   `uv run ty check packages/tkati-node-el`, `uv run pytest packages/tkati-node-el`.

## Implementation notes (as built)

Built as designed; the differences:

- **The "must define" error is a `TypeError`**, not the `ValueError` in
  *Loading*: ruff's `TRY004` asks for `TypeError` after a `callable()` check,
  and a non-callable `transform` is a type problem. Both it and the non-table
  return are `TypeError`s.
- **The loop times through `node.phase("transform")`**, the harness's public
  hook that `tkati-node-dedup` uses, rather than `node.stats.phase(...)`. Same
  `LoopStats`, same result.
- `run()`'s `Produced N rows` debug line reports the rows sent (after the
  transform), not the rows read.
- `test_transform.py` holds the README example as a TOML string and loads it
  through `AppSettings(**tomllib.loads(...))`; `SETTINGS_FILE` is read at
  import time, so a test can't point `AppSettings()` at a file of its own.
- The ClickHouse roundtrip case (`test_node_el_polars_transform_lands_in_clickhouse`)
  adds `traffic_total UInt64` to the test table with `ALTER TABLE` and asserts
  inside the transform that `uid` reaches the producer as `large_string`.

## Verification

- `uv run pytest packages/tkati-node-el`: 20 passed, against a local Redpanda
  and ClickHouse (including the new roundtrip).
- `uv run ruff check packages/tkati-node-el` and
  `uv run ty check packages/tkati-node-el`: clean.
- The README example, run through `tkati_core._native.encode_arrow`, prints
  the two JSON lines shown in *Writing a transform* (with a different
  `ingested_at`).
