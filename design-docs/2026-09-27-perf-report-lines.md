---
status: IMPLEMENTED
---

# Perf report: a loop line and a read line

## Context

`LoopStats.report()` (`packages/tkati-core/tkati_core/stats.py`) logged every
phase on one line, each as a share of the interval's wall clock:

```
perf: consumer/poll=4.43s (44%) consumer/parse=0.48s (5%) wait/input=0.21s (2%) lookup=0.52s (5%) producer/serialize=2.10s (21%) producer/enqueue=0.35s (4%) producer/deliver=0.00s (0%) wait/in-flight=1.12s (11%) write=0.21s (2%) commit=0.38s (4%)
```

That was built for a single-threaded loop. Since read-ahead
(`design-docs/2026-09-26-pipelined-worker-loop.md`), phases come from two
threads:

- the read-ahead thread (`_NodeBase._read_ahead_loop`): `consumer/poll` and
  `consumer/parse`;
- the loop thread: `wait/input` (`_NodeBase._take`), the node's own phases,
  `producer/serialize` and `producer/enqueue` (inside `done()`),
  `wait/in-flight` (`PipelinedNode._delivered`) and `commit`.

On one line their shares overlap and can add up past 100%. The line no longer
answers the question it is for: what the loop spends its time on.

## Goal

Done when:

- The node's report has a `perf loop:` line with only loop-thread phases, in
  the order they happen, and a `perf read:` line with the reader's.
- The read line shows how long the reader waited for the loop, so a
  loop-bound node is visible from the reader's side too.
- `wait/input` means "the loop waited for its next event" with or without
  read-ahead.

Non-goals:

- A write line. The write work that is timed (serialize, enqueue, commit)
  runs on the loop thread. Delivery is async inside librdkafka, and
  `wait/in-flight` already shows when it is the bottleneck. Timing delivery
  itself would need a new per-batch measurement.
- Exporting reading's phases as Prometheus metrics. Only the loop's stats are
  exported for now.

## Approach

One stats object per thread. `LoopStats` did two jobs: timing phases, and
counting the loop's rows and iterations under a `perf over` header. The
timing part is split out as `PhaseStats`, which logs one `perf <label>:` line.
`LoopStats` becomes a `PhaseStats` with the counters on top. The consumer and
producer take a `PhaseStats`, so a `LoopStats` still fits wherever one did.

The node owns both: `node.stats`, the loop's `LoopStats`, and
`node.read_stats`, a `PhaseStats` for reading. It hands the consumer
`read_stats` and the producer `stats`. It reports `read_stats` whenever
`stats` reports, so the two lines cover the same interval. Keeping reading
out of the loop's stats also keeps it out of `/metrics`, which export the
loop's stats only.

With read-ahead, the read stats also carry `wait/loop`: time the reader spent
blocked handing a batch over because the queue was full. Without read-ahead
the loop reads synchronously, and that read is timed as the loop's
`wait/input`. The read line then breaks the same time down into poll and
parse.

A node's `phases=` must not list `CONSUMER_PHASES` any more. It raises rather
than silently moving them, because they'd otherwise sit on the loop line
reading 0.

An earlier draft of this change kept one `LoopStats` with named "side lines"
of phases left out of the main line. It worked, but it needed filtering logic
and silently moved listed consumer phases. One object per thread says the
same thing without either.

## Implementation notes (as built)

- `PhaseStats` holds `phases`, `label`, `phase_sec`, `started`, the phase
  totals and the lock. It has `record`, `phase`, `reset` (via an overridable
  `_fold`) and `report`. `LoopStats(PhaseStats)` adds `report_interval_sec`,
  the counters, `totals()` and the `perf over` header. `report_if_due()` now
  returns whether it reported, which the node uses to report `read_stats`
  alongside.
- The lock is still taken by `record()`: the reader records into
  `read_stats` while the loop thread resets it.
- `_NodeBase.__init__` builds `read_stats` with `CONSUMER_PHASES`, plus
  `wait/loop` when `read_ahead > 0`. `CONSUMER_PHASES` left the `*_PHASES`
  constants, which are now in chronological order.
- `wait/loop` is recorded in `_hand_over`'s `finally`. `wait/input` wraps the
  synchronous `_read` in `_next_event`.
- node-dedup's `_PHASES` puts `write` after `commit`: it runs in
  `after_commit`.

## Verification

- `test_stats.py`: a `PhaseStats` logs one line named by its label, and a
  labelled `LoopStats` logs its header, then `perf loop:`.
- `test_node.py`:
  - every loop report comes with a read report;
  - consumer phases are on the read line only, and `phases=` listing them
    raises;
  - `wait/input` is timed without read-ahead;
  - a slow loop body shows up as `wait/loop` in `read_stats`;
  - the consumer is handed `read_stats` and the producer `stats`.
- ruff, ty and the tkati-core, node-dedup and node-el test suites pass.
