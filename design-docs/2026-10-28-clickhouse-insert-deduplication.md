---
status: IMPLEMENTED
---

# ClickHouse insert identity across retries

## Context

`ClickhouseProducer.from_output_settings` in
`packages/tkati-core/tkati_core/clickhouse/producer.py` creates a synchronous
ClickHouse Connect client with automatic HTTP sessions. If an HTTP insert
times out while the server continues executing it, `_insert_with_retry` sends
the next attempt with the same session. The server can reject it with
`SESSION_IS_LOCKED` (373). Disabling sessions permits another attempt but does
not identify it as a replay of the original write.

The existing error classifier and recursive DLQ policy are described in
`design-docs/2026-09-28-clickhouse-error-classification.md`. A data error enters
`_insert_with_dlq_fallback`, which resends the full table before splitting it.
Successful child inserts can themselves time out and need retries.

The `tag` argument is a process-local delivery counter (`_tag` in
`tkati_core/node.py`); different producer lifetimes can reuse it. Neither tag
nor a hash of rows can identify a logical insert safely: independent calls
may legitimately write identical data.

## Goal

Done when:

- Clients created from output settings send inserts without an HTTP session.
- All attempts of one logical insert use the same nonempty deduplication token.
- Independent produce calls have distinct identities even with identical rows
  and the same delivery tag.
- Each child created by DLQ splitting has an independent identity, stable
  across its attempts; resending the original full batch keeps its identity.
- Tests cover a write that committed before its HTTP response timed out.

Non-goals:

- Persisting identity across process restarts, separate produce calls, or a
  Kafka rewind. The existing node remains at-least-once.
- Preventing every duplicate across a partially committed parent and its
  differently grouped DLQ child inserts.
- Changing the retry classifier, attempt budget, backoff, or DLQ delivery.
- Altering tables, server profiles, or deduplication windows.
- Claiming exactly-once delivery independent of ClickHouse settings.

## Approach

Assign a UUID once at the boundary of a logical produce call, before entering
the decorated retry helper. Pass the identity through every attempt using the
query setting `insert_deduplication_token`. Send `insert_deduplicate=1` to
enable the synchronous insert deduplication switch used by the deployed 24.1
server. Do not change global driver settings.

Derive child tokens from the parent's identity and the child's position in
the split tree. This keeps retry identities stable while avoiding accidental
suppression of independent child inserts, including identical rows.

## Design

`produce_arrow` creates `uuid4().hex` once, inside its deliver phase, and passes
it to both `_insert_with_retry` and the initial `_insert_with_dlq_fallback`.
`produce_pylist` delegates to `produce_arrow`, so it follows the same contract.

The private retry helper requires a token. It never creates or regenerates
one and passes a fresh settings dict to `Client.insert_arrow` on each attempt.
The recursive helper also requires a token. Child `i` gets
`f"{parent_token}/{i}"`, a unique path within the insertion's split tree.
The root fallback resend receives the original token, not a new UUID.

`from_output_settings` passes `autogenerate_session_id=False`. Externally
supplied clients remain the caller's responsibility.

ClickHouse must support insert deduplication and retain the token within its
deduplication window. On the deployed 24.1 server, asynchronous inserts need
their own compatible deduplication configuration; enabling the synchronous
switch alone does not make them safe. A token is neither a transaction nor a
row-level uniqueness constraint. Default expressions such as `now()` are not
used to construct the client token.

Reference: [ClickHouse insert retry deduplication](https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries).

## Implementation Steps

1. Update `ClickhouseProducer.from_output_settings` to disable automatic HTTP
   sessions.
2. Add a required `deduplication_token` parameter to `_insert_with_retry`; send
   the token and `insert_deduplicate=1` as insert settings.
3. Create one UUID in `produce_arrow`, reuse it for the root fallback, and
   derive child tokens in `_insert_with_dlq_fallback`.
4. Update `packages/tkati-core/tests/test_ch_producer.py` for the helper
   contract and add HTTP timeout-after-commit, independent insert, DLQ split,
   and pylist retry cases.
5. Document the replay scope and server requirements in
   `packages/tkati-core/README.md`, the package changelog, and root changelog.

## Implementation notes (as built)

The public producer interface is unchanged. Required token parameters belong
only to the two private ClickHouse insert helpers. Tokens are created before
the tenacity-decorated helper and are never regenerated inside a retry.

## Verification

All 31 producer tests pass. The HTTP regression uses the real
driver with server discovery and transport simulated; it does not claim
verification against a running SharedMergeTree server.

Together with `packages/tkati-core/tests/test_node.py`, 105 tests pass.
`ty check packages/tkati-core`, Ruff on the changed Python files, and
`git diff --check` pass. Ruff on the entire package reports the pre-existing
`call-datetime-without-tzinfo` finding at `tests/test_native_codec.py:75`; that
file matches HEAD and was not changed.
