---
status: IMPLEMENTED
---

# ClickHouse insert failures: bad rows versus a bad server

## Context

`ClickhouseProducer` (`packages/tkati-core/tkati_core/clickhouse/producer.py`)
treated every insert failure alike. `_insert_with_retry` retried any `Exception`
3 times 1 second apart, and `_insert_with_dlq_fallback` split the batch on any
`Exception`, recursing to single rows with those retries re-applied at every
level.

This is audit finding **F3** (`design-docs/2026-09-27-tkati-core-audit.md`),
severity High. It has two halves:

- **A parse error was retried pointlessly.** A row ClickHouse cannot parse will
  never parse. Isolating one bad UUID in a 1,000-row batch took about 1,100
  inserts × 2 seconds of sleep — over half an hour.
- **An outage emptied the batch into the DLQ.** Connection refused and timeouts
  also triggered the split, so every row was eventually filed as "rejected" and
  the batch committed. An outage produced a DLQ full of good data.

The driver gives us what we need to tell these apart, but not where you would
first look. `HttpClient._error_handler`
(`clickhouse_connect/driver/httpclient.py`) picks the exception class by whether
the request had been retried, not by what went wrong —
`err_type = OperationalError if retried else DatabaseError` — and sets `code`
from the `X-ClickHouse-Exception-Code` header on both branches. A pure transport
failure raises `OperationalError` with `code=None`. So **the class carries no
information and the code carries all of it.** `name` is only populated when
`show_clickhouse_errors` is on, which makes it fit for log lines and nothing
else.

## Goal

Done when:

- A row ClickHouse rejects for its content reaches the DLQ without any retry
  sleep, and the good rows around it still land in ClickHouse.
- A ClickHouse outage never puts a row in the DLQ. The batch fails instead, so
  the node rewinds and re-reads it.
- Schema drift — a missed migration — stops the node rather than quarantining
  whole batches.
- The policy is legible: a reader can see which error codes count and why the
  others don't.

Non-goals:

- A settings knob for the code set. Belongs with the audit's F5 ("settings can't
  be configured") and has no requester; rebinding `CH_DATA_ERROR_CODES` at
  startup is the escape hatch in the meantime.
- A longer retry budget. Kept at 3 attempts 1 second apart (see Consequences).
- F2, a failed Kafka DLQ write losing rows silently. Adjacent and Critical, but a
  change in `src/kafka.rs`.
- A counter for DLQ'd rows.

## Approach

One classifier, keyed on the ClickHouse server error code, drives **both**
decisions — whether to retry and whether to split. That pairing is the whole
design, because the two questions have the same answer: an error caused by the
rows is worth no retry and worth isolating, and an error caused by the server is
worth retrying and worth failing the batch over. Splitting them into two
policies would let them disagree.

The classifier is `_is_data_error(err)`: an instance of the driver's `Error` whose
`.code` is in `CH_DATA_ERROR_CODES`. It is used in three places — as tenacity's
`retry=` predicate, and as a guard at the top of each of the two `except` blocks.

Consequently **the split never retries a data error**, not because the descent
was given a different insert helper, but because the one insert helper doesn't
retry data errors. There is a single insert path with a single policy. Stated
explicitly here because "retry inside the split" is the obvious reading someone
will re-introduce: it was the bug.

Keeping the descent on the retrying helper also has a benefit. A transient blip
discovered mid-descent — ClickHouse restarting between chunks — still gets its 3
attempts at that node and can recover, so the descent finishes instead of
throwing away the isolation work already done.

### Consequences accepted

- **A ClickHouse restart longer than ~2 seconds now bounces the process.** The
  old code (wrongly) absorbed an outage into the DLQ; the batch now fails, and
  `tkati_node_el.main` has no retry loop around its `with` block, so the process
  exits and the backoff is the supervisor's. This was a deliberate choice over
  raising the budget. If it proves noisy, the fix is `wait_exponential` with a
  `stop_after_delay`, and the numbers belong on `ClickHouseOutputSettings` next
  to `dlq_split_factor`.
- **A client-side insert failure is now a poison batch.** `Client.insert_arrow`
  calls `arrow_buffer()` before any HTTP request; if that raises — an unsupported
  Arrow type — the exception has no `.code`, so it is non-data: retried, raised,
  rewound, and failed again identically forever. Previously it drained to the DLQ
  row by row. This is F1's poison-batch shape on the producer side, and it is
  availability traded for correctness. Narrowing it would mean re-admitting some
  set of client-side errors to the DLQ path, and there is no principled way to
  pick that set.
- **The DLQ is at-least-once.** A non-data error found mid-descent unwinds the
  whole descent, but rows already sent to the DLQ stay sent (the node's
  `__exit__` closes the DLQ, and `KafkaProducer.close()` flushes). After the
  rewind they are read, rejected and filed again. Wrapping the descent in
  `try/finally: dlq.flush()` would *guarantee* those duplicates rather than
  merely risk them, so it is deliberately absent.
- **Tracebacks chain.** Each level re-raises from inside an `except`, so a
  mid-descent non-data error arrives with a `During handling of the above
  exception` chain of data errors, one per level (at most 4 at
  `split_factor=10`). Informative, not noise — don't "fix" it with
  `raise ... from None`.

## Design

`CH_DATA_ERROR_CODES` is public — the *policy* is the part an operator may
legitimately disagree with, and because `_is_data_error` reads the module global
at call time, `CH_DATA_ERROR_CODES |= {53}` at startup is an honest escape hatch.
It stays a `frozenset` so it is rebound, never mutated under a running descent.
`_is_data_error` is private: an implementation detail of one module, with no
known caller.

Included — parse and value errors, each one a fault of an individual row. Names
verified against `errorCodeToName` in `clickhouse local`:

| Code | Name | Code | Name |
| --- | --- | --- | --- |
| 6 | `CANNOT_PARSE_TEXT` | 321 | `VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE` |
| 25 | `CANNOT_PARSE_ESCAPE_SEQUENCE` | 349 | `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN` |
| 26 | `CANNOT_PARSE_QUOTED_STRING` | 376 | `CANNOT_PARSE_UUID` |
| 27 | `CANNOT_PARSE_INPUT_ASSERTION_FAILED` | 407 | `DECIMAL_OVERFLOW` |
| 38 | `CANNOT_PARSE_DATE` | 434 | `CANNOT_PARSE_PROTOBUF_SCHEMA` |
| 41 | `CANNOT_PARSE_DATETIME` | 441 | `CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING` |
| 72 | `CANNOT_PARSE_NUMBER` | 467 | `CANNOT_PARSE_BOOL` |
| 117 | `INCORRECT_DATA` | 632 | `UNEXPECTED_DATA_AFTER_PARSED_VALUE` |
| 128 | `TOO_LARGE_ARRAY_SIZE` | 675 | `CANNOT_PARSE_IPV4` |
| 131 | `TOO_LARGE_STRING_SIZE` | 676 | `CANNOT_PARSE_IPV6` |

Excluded on purpose: 53 `TYPE_MISMATCH`, 16 `NO_SUCH_COLUMN_IN_TABLE`,
8 `THERE_IS_NO_COLUMN`, 9 `SIZES_OF_COLUMNS_DOESNT_MATCH`,
20 `NUMBER_OF_COLUMNS_DOESNT_MATCH`, 43, 44, 50, 70. These are schema-shape
errors: they reject every row in the batch alike, so calling them data errors
would drain whole batches into the DLQ one row at a time while the real problem —
a missed migration — went unnoticed. Failing loudly is the correct response, and
it is why the set is "parse and value errors" rather than "everything the server
blames on the request".

Note `_is_data_error` is unrelated to the driver's own DB-API `DataError` class,
which can be raised client-side with no code and so counts as *not* a data error
here. The docstring says so; it is a live trap.

### Resulting behaviour

| Failure | Retries | Splits to DLQ | Outcome |
| --- | --- | --- | --- |
| Parse or value error (a code in the set) | none | yes | bad rows in the DLQ, good rows in ClickHouse |
| Connection refused, timeout, auth, `code=None` | 3 × 1s | no | raised; batch rewound and re-read |
| Schema error (53, 16, …) | 3 × 1s | no | raised; node stops until the table is fixed |
| Data error with no DLQ configured | none | n/a | raised; nowhere to put the row |

## Implementation notes (as built)

`packages/tkati-core/tkati_core/clickhouse/producer.py`:

- `CH_DATA_ERROR_CODES`, `_INSERT_ATTEMPTS = 3`, `_is_data_error`, and
  `_describe` are new. `_describe` appends `[code <n> <name>]` to a log line,
  because with `show_clickhouse_errors=False` `str(err)` degrades to "The
  ClickHouse server returned an error" while `.code` survives — and the code is
  exactly what an operator needs to decide whether it belongs in the set.
- `_insert_with_retry` gained
  `retry=retry_if_exception(lambda err: not _is_data_error(err))`. When the
  predicate returns `False`, tenacity's action chain is
  `rs.outcome.result()`: the original exception is re-raised immediately, before
  the wait, the stop check and `before_sleep`. So a data error costs one
  `insert_arrow` call, no sleep, no `RetryError` wrapper and no retry log line.
  Verified: 1 call in 0.00s for a data error, 3 calls over 2.00s otherwise.
  `reraise=True` still governs the exhausted-`stop` path. The name is unchanged —
  it still retries, just not data errors.
- `log_retry_attempt`'s hardcoded `/3` now comes from `_INSERT_ATTEMPTS`, so the
  number is stated once. Not read from
  `retry_state.retry_object.stop.max_attempt_number`, which would need a `cast`
  past `ty`'s `stop_base` type for nothing.
- Each of the two `except Exception as err:` blocks opens with
  `if not _is_data_error(err): ... raise`. Raising from inside an `except` is not
  caught by that same `try`, and the recursive calls sit inside the `except`, so
  one guard per handler unwinds the entire descent, bypasses every parent frame's
  handler, and logs exactly once. No extra plumbing was needed for propagation.

`clickhouse-connect` floor raised from `>=0.11.0` to `>=1.4.2` in
`packages/tkati-core/pyproject.toml` and `packages/tkati-node-el/pyproject.toml`.
`Error.__init__(*args, code=..., name=...)` is what this design rests on; on a
resolution without it, `_is_data_error` would raise `AttributeError` *inside the
retry predicate*, which tenacity does not catch, turning every insert failure
into an `AttributeError` with the real error as `__context__`. Preferred to
`getattr(err, "code", None)`, which would instead degrade silently to "nothing is
a data error".

## Verification

`packages/tkati-core/tests/test_ch_producer.py` grew to 28 tests. Ten existing
ones changed: they used a bare `Exception` to reach the DLQ, which is precisely
the behaviour this change removes, so each now names which kind of failure it
means via `_ch_data_error(code=376)` or `_ch_transport_error()`. The new ones
pin the decision table (`test_is_data_error_keys_off_the_server_code`, including
`53 not in CH_DATA_ERROR_CODES`), the absence of sleeps
(`test_data_error_splits_without_sleeping` — the F3 regression test), the outage
path (`test_connection_error_is_retried_then_fails_the_batch`, asserting the DLQ
is never touched), the `code=None` rule independent of class, and the mid-descent
abort.

`packages/tkati-node-el/tests/test_node_el.py` gained
`test_ch_producer_isolates_a_bad_row_against_real_clickhouse`, on a new
`ch_uuid_table` fixture. The unit tests only check our model of the driver; this
one is the sole end-to-end evidence that a real ClickHouse rejecting a real bad
UUID produces an exception carrying code 376. Confirmed by hand against the
devcontainer's server: `DatabaseError`, `code=376`, `name="CANNOT_PARSE_UUID"`.
The transport-error shape (`OperationalError`, `code=None`) was confirmed the
same way, but has no integration test: `get_client` pings on construction, so a
dead port fails before a producer can be built.

All 180 `tkati-core` tests and 6 `tkati-node-el` tests pass, with
`ruff check` and `ty check` clean.
