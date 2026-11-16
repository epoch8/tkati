---
status: IMPLEMENTED
---

# Duplicate field counters

## Context

`packages/tkati-node-dedup/src/tkati_node_dedup/main.py` exposes
`_DROPPED_ROWS`, incremented by `_mark_seen` after a batch commits. Operators
can count discarded events but cannot measure discarded traffic from fields
such as `traffic_in` and `traffic_out`. `DedupSettings` currently configures
only the dedup key, window and store. The commit boundary follows
`design-docs/2026-09-26-pipelined-worker-loop.md`.

## Goal

Done when:

- Operators can configure input fields and see their sums on duplicate rows
  in Prometheus.
- Missing fields and unsuitable values contribute zero without stopping the node.
- Only committed batches contribute, including duplicates in the batch,
  pending batches and the persisted store.
- Existing deployments need no configuration change.

Non-goals:

- Counting survivor traffic or changing deduplication and delivery semantics.
- Persisting metrics across restarts or exposing nested payload paths.
- Changing package versions.

## Approach

Reuse the existing keep mask to aggregate only discarded values, then carry
scalar sums into the existing commit callback. Expose a single Counter family
with a field label, avoiding dynamic metric names and registration collisions.
Create each configured series at startup so missing fields still expose zero.
No extra filtering or aggregation runs when the configuration is empty.

## Design

`dedup.counter_fields` is a list of input column names, defaulting to empty.
Environment configuration uses the existing JSON list convention.
`tkati_node_dedup_dropped_field_total{field="traffic_in"}` sums values in the
input column's units. Duplicate configuration entries are counted once.

Only integer, floating-point and decimal columns contribute. Aggregate using
Arrow kernels in float64, matching Prometheus counter precision and avoiding
integer overflow. Nulls, negative numbers, NaN and infinities contribute zero;
unsupported column types and missing columns also contribute zero. A sum
overflowing float64 contributes zero. Calculate from discarded rows directly
instead of subtracting survivor totals from input totals.

## Implementation Steps

1. Add `DedupSettings.counter_fields` in `settings.py` and pass it from `main` to `run`.
2. Register `_DROPPED_FIELDS` in `main.py`; initialize configured label series in `run`.
3. Extend `_dedupe_batch` to return discarded field sums using its existing mask.
4. Extend `_mark_seen` to increment these counters after commit alongside `_DROPPED_ROWS`.
5. Document TOML, environment settings and PromQL in the package README and changelog.
6. Verify aggregation, invalid values, pending duplicates, delivery failures,
   zero series and configuration; run package pytest, Ruff and ty checks.

## Implementation notes (as built)

`run` deduplicates configured names and initializes label series before
consumption. `_dedupe_batch` aggregates individual dropped columns with the
inverse keep mask and returns only scalar sums. `_mark_seen` updates the
counters alongside the existing dropped-row counter. `main` passes configured
fields into `run`. Arrow publishes the compute kernels dynamically, so narrow
`Any` casts document runtime members that `ty` cannot resolve.

## Verification

- 52 broker-free package tests passed, including 22 new cases covering
  field counters, settings overrides, the main entry point and chunked columns.
- Package Ruff and `ty` checks passed.
- Five Kafka integration tests require Redpanda on `localhost:9092`. The full
  run was interrupted waiting for that unavailable broker; these tests were
  excluded from the successful broker-free run.
