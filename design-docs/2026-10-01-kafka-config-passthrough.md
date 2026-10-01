---
status: IMPLEMENTED
---

# Kafka client config: a passthrough with reserved properties

## Context

Every Kafka client this workspace builds is configured from a handful of typed
fields and nothing else.

`KafkaProducer.from_topic_settings`
(`packages/tkati-core/tkati_core/kafka/producer.py`) sets exactly one property:

```python
kafka_config={"bootstrap.servers": connection.broker}
```

`KafkaConsumer.from_input_settings`
(`packages/tkati-core/tkati_core/kafka/consumer.py:65`) sets four:
`bootstrap.servers`, `group.id`, `auto.offset.reset` and
`enable.auto.commit=False`. Everything else librdkafka exposes — several hundred
properties — is unreachable from `settings.toml`. No compression, no
`linger.ms`, no `acks`, no `client.id`, no idempotence, no authentication.

This is audit finding **F5** (`design-docs/2026-09-27-tkati-core-audit.md`),
severity Medium, and the configurability half of **E5** ("throughput is left on
the table by configuration"). Both are blocked on it: today every knob anyone
wants means editing `from_topic_settings` and shipping a release.

Compression is the concrete case that prompted this. In the default `"json"`
format `produce_arrow` emits **one message per row**, each a JSON object
repeating every column name (`encode_arrow`,
`packages/tkati-core/src/lib.rs:203`). A 1 000-row batch is 1 000 messages with
identical keys — close to the best case for a block compressor — and all of it
is currently paid for in full at the broker's disk, its page cache, and every
hop between the nodes of a dataflow. `compression.type` is one librdkafka
property, and there is no way to set it.

Each client wrapper takes a `kafka_config` dict already, and `config_entries`
(`packages/tkati-core/src/lib.rs:48`) accepts any Python value, stringifying it
and lowering bools to librdkafka's `true`/`false`. **The plumbing exists end to
end; what is missing is a way into it from settings.**

### What librdkafka does and does not catch

Probed against the built extension, because the design depends on it:

| Config passed to `NativeProducer` | Result |
| --- | --- |
| `{"compresion.type": "zstd"}` (typo'd key) | `KafkaError: No such configuration property: "compresion.type"` |
| `{"compression.type": "brotli"}` (bad value) | `KafkaError: Invalid value "brotli" for configuration property "compression.codec"` |
| `{"linger.ms": 50}` (int) | accepted |
| `{"enable.idempotence": True}` (bool) | accepted |

So a passthrough is **not** the unvalidated option it is usually taken for:
librdkafka rejects unknown properties *and* bad values at client construction,
naming the property in both cases. In a node that is `build_producer` during
`_NodeBase.from_settings`, milliseconds after settings parse — the same startup
in which a pydantic error would have fired.

Two things it does not catch, both load-bearing for this design:

| Config passed to `NativeConsumer` | Result |
| --- | --- |
| `{"linger.ms": 50}` (producer-only property) | accepted; `%4\|CONFWARN\| ... queue.buffering.max.ms is a producer property and will be ignored by this consumer instance` |
| `{"enable.auto.commit": True}` | accepted, silently |

The CONFWARN goes to librdkafka's own log, not loguru, so in a running node it is
effectively invisible. And `enable.auto.commit` is the property tkati's entire
commit protocol rests on.

## Goal

Done when:

- Any librdkafka property can be set per client from `settings.toml`, without a
  code change in `tkati-core`. Compression included, as the first user of it.
- A property tkati itself sets cannot be silently overridden from settings.
  Attempting it is an error that names the property and says why.
- Values may be strings, ints or bools in TOML, and reach librdkafka in the form
  it wants.
- The reserved set is legible: a reader can see which properties tkati owns and
  which of those are owned for correctness rather than convenience.
- `settings.toml` examples exist for the three things operators will reach for
  first: compression, producer throughput tuning, and `SASL_PLAINTEXT`/`PLAIN`
  auth.

Non-goals:

- **Changing any default.** In particular `enable.idempotence` stays at
  librdkafka's `false`. That default is the actual bug inside F5 — with unlimited
  retries and in-flight requests, a retried send can land after a later one and
  can duplicate, which breaks the per-key ordering `key_column` exists to
  provide — and a passthrough does not fix it, because it only helps operators
  who already know to set it. It is a behaviour change with its own throughput
  consequences and its own verification, and it should be bisectable on its own.
  Separate change, landing straight after this one. The same goes for defaulting
  compression to `lz4` or `zstd`, which F5 also recommends: it would re-encode
  every existing deployment's traffic on upgrade and raise producer CPU without
  anyone asking.
- **A typed field for compression**, or for any other property. See
  *Why compression is not a typed field*.
- **TLS and the stronger SASL mechanisms.** Not a settings problem: the
  extension is built with `rdkafka = { default-features = false, features =
  ["libz-static", "zstd"] }` (`packages/tkati-core/Cargo.toml:28`). Probed:
  `SASL_PLAINTEXT` + `PLAIN` works today; `SCRAM-SHA-256`, `GSSAPI` and
  `OAUTHBEARER` fail with *"No provider for SASL mechanism … recompile
  librdkafka with libsasl2"*, and any `_SSL` protocol with *"OpenSSL not
  available at build time"*. Re-enabling needs `ssl-vendored` plus
  `perl-IPC-Cmd` in the publish workflow, as the comment above that dependency
  already documents. This change makes those properties *settable*; it does not
  make them *work*.
- Per-topic or per-partition config. librdkafka's topic-level properties are
  accepted in the same flat namespace, so they come along for free, but nothing
  is designed for them here.
- Validating property names against a vendored list of librdkafka properties.
  librdkafka does it, better, and a list in Python would go stale.
- Surfacing the dict in `tkati-dashboard`. It renders a dataflow graph, not a
  config audit.
- Secret handling. `sasl.password` in a `settings.toml` is as exposed as the rest
  of that file; this change does not introduce a secrets story, and the README
  should not imply one.

## Approach

One `config` dict per client-bearing settings block, merged over the properties
tkati derives from its typed fields, with those properties held back in a
**reserved set** that the dict may not name.

The merge is a layering with three steps, and the third is what makes it honest:

1. Properties tkati derives from typed fields — `bootstrap.servers` from
   `connection.broker`, `group.id` from `consumer.group_id`, and so on.
2. The operator's `config` dict, merged over them.
3. A parse-time validator rejecting any key in step 2 that step 1 would also
   have set.

Step 3 means steps 1 and 2 can never actually collide — the merge order is
arithmetic rather than policy. That is deliberate. "Merged last wins" is what
F5's recommendation proposes, and it is wrong here for one specific reason:
`enable.auto.commit=True` is accepted silently by librdkafka (see Context), and
the consumer's explicit-commit protocol — `commit(batch)` after a batch is
durable, rewind on failure — is built on it being `False`. A passthrough that can
override it turns a node's failure path into silent data loss, configurable by
someone who has no reason to suspect it. The same argument, more weakly, covers
`bootstrap.servers`, `group.id` and `auto.offset.reset`: they have typed homes,
and two sources for one value is a bug waiting for a reader.

Rejecting rather than warning is for the same reason. A warning about
`enable.auto.commit` would land in the same place librdkafka's CONFWARN does —
a log nobody reads until afterwards.

## Design

### Shape and placement

```toml
[output]
type = "kafka"

[output.config]
"compression.type" = "zstd"
"linger.ms" = 50
"batch.num.messages" = 50000
"client.id" = "dedup-prod-1"
```

`config: dict[str, str | int | bool] = {}` goes on `KafkaInputSettings` and
`KafkaOutputSettings` — the per-role blocks — **not** on
`KafkaConnectionSettings`, which is where F5 recommends it.

The reason is the CONFWARN probe. A producer-only property on a consumer is
accepted, ignored, and warned about only on librdkafka's own log.
`KafkaConnectionSettings` is shared in shape by `KafkaInputSettings` and
`KafkaOutputSettings`, so putting the dict there would create exactly one place
in the settings schema where a knob can be set, look correct, and do nothing.
The same trap rules out `KafkaTopicSettings`, which input and output share
outright: `compression.type` under a topic read by a consumer would be dead
config, and compression is a property of a producer's connection to the broker
rather than of the topic — the broker's own default `compression.type=producer`
means it stores whatever each producer sent, so two producers may legitimately
write one topic with different codecs.

Per-role placement also lets the reserved set differ by role, which it must:
`enable.auto.commit` is meaningless on a producer and load-bearing on a consumer.

The cost is that an operator using SASL repeats the credentials in `[input]` and
`[output]` (and `[dlq]`). That is already true of `broker`, and TOML has no
anchors, so the alternative is a shared block that would need its own merge
rules. Accepted.

Because `[dlq]` is a full `OutputSettings` (`NodeSettings.dlq`,
`packages/tkati-core/tkati_core/settings.py:93`), a Kafka DLQ gets `config` with
no extra plumbing.

### The reserved sets

Two module-level frozensets in `tkati_core/kafka/settings.py`, public for the
same reason `CH_DATA_ERROR_CODES` is
(`design-docs/2026-09-28-clickhouse-error-classification.md`): the policy is the
part a reader may want to inspect.

```python
PRODUCER_RESERVED = frozenset({"bootstrap.servers"})
CONSUMER_RESERVED = frozenset({
    "bootstrap.servers", "group.id", "auto.offset.reset", "enable.auto.commit",
})
```

Each entry is reserved for one of two distinct reasons, and the docstring should
say which, because they have different futures:

- **Owned for correctness** — `enable.auto.commit`. tkati's commit protocol
  requires it. This one stays reserved forever, and it is the reason the set
  exists rather than a merge order.
- **Owned by a typed field** — `bootstrap.servers`, `group.id`,
  `auto.offset.reset`. Reserved only because something else already sets them;
  the field is the way to set them. If a typed field is ever removed in favour of
  the passthrough, its property leaves the set with it.

Exactly one property librdkafka would let you set is *dangerous* rather than
merely *duplicated*, which is worth stating plainly: a short reserved set is a
feature, and the instinct to pre-emptively reserve `acks`, `retries`,
`enable.idempotence` or `max.in.flight.requests.per.connection` should be
resisted. Those are the knobs this change exists to open.

### Why compression is not a typed field

A `compression: Literal["none", "gzip", "snappy", "lz4", "zstd"]` field on
`KafkaOutputSettings` is the obvious alternative, and it was the first plan. It
is not worth it, for a reason the probe table settles: librdkafka answers
`compression.type = "brotli"` with *`Invalid value "brotli" for configuration
property "compression.codec"`* at client construction, which is the same startup
as the pydantic error, with the same information in it. The `Literal` buys a
slightly better message and a settings schema that enumerates the five codecs.

Against that, a typed field costs a second mechanism for one property. Both would
be able to set `compression.type`, so either a precedence rule exists for a
reader to remember, or `compression.type` joins the reserved set and the two
spellings become a startup error — machinery to explain either way. And the
precedent is the real cost: every subsequent property becomes an argument about
whether it deserves a field, and the ones that lose are reachable only through a
dict that the typed ones imply is second-class.

So there is one mechanism. What was going to be the field's docstring becomes
README guidance instead, which is where the non-obvious part lives anyway: **the
codec earns much more under `format = "json"`**, where it sees many small,
near-identical messages in one message set, than under `"arrow-batch"`, a single
already-compact Arrow IPC message per table with no cross-message redundancy to
exploit. Both are compressed; only one is transformed.

### Validation and errors

A pydantic model validator on each of the two settings classes, raising with the
offending key and its reason:

```
output.config: "enable.auto.commit" is managed by tkati and cannot be set here;
the node commits each batch explicitly after it is durable.
input.config: "group.id" is set from `input.consumer.group_id`; set it there.
```

Two messages, one per reason, so the reader learns which kind of reservation they
hit. Validation is per-block and names the block, because a node with
`[input]`, `[output]` and `[dlq]` can trip it in three places.

Everything else is librdkafka's to validate, at client construction, with the
messages in the Context table. This is the one place the design accepts a
less-good error message than pydantic would give, and it buys the entire point of
the change: we never have to know the property list.

### Types

`dict[str, str | int | bool]` matches what TOML yields for the properties people
actually set (`"linger.ms" = 50`, `"enable.idempotence" = true`,
`"client.id" = "x"`), and `config_entries` already lowers all three correctly —
bools via `PyBool` to `true`/`false`, the rest via `str()`. Floats are excluded:
librdkafka has no float-valued property, and allowing them would only let
`"linger.ms" = 50.0` through as `"50"`.

`KafkaProducer.__init__`'s `kafka_config: dict[str, str]` must widen to
`dict[str, str | int | bool]`. `KafkaConsumer.__init__` is already
`dict[str, str | bool]` and widens to match, so the long-standing asymmetry
between the two wrappers goes away. `_native.pyi` already types the parameter as
`Mapping[str, object]` and needs no change.

### The typed fields that remain

`broker`, `group_id`, `auto_offset_reset`, `batch_size` and `batch_timeout_sec`
stay typed. They are the shape of a node — what it connects to and how it
batches — not tuning, and `batch_size` / `batch_timeout_sec` are read by tkati's
own poll loop rather than passed to librdkafka at all.

Nothing new gets a field. `compression.type`, `linger.ms`, `compression.level`,
`batch.num.messages`, `batch.size`, `acks`, `client.id` and `fetch.*` are
passthrough keys.

## Implementation Steps

1. `packages/tkati-core/tkati_core/kafka/settings.py`
   - Add `KafkaConfigOverrides = dict[str, str | int | bool]`,
     `PRODUCER_RESERVED` and `CONSUMER_RESERVED` frozensets, each with the
     two-reasons docstring.
   - Add `config: KafkaConfigOverrides = Field(default_factory=dict)` to
     `KafkaInputSettings` and `KafkaOutputSettings`.
   - Add a shared `_reject_reserved(...)` helper and a
     `@model_validator(mode="after")` on each class calling it.
2. `packages/tkati-core/tkati_core/kafka/producer.py`
   - Widen `__init__`'s `kafka_config` to `dict[str, str | int | bool]`.
   - `from_topic_settings` takes `config: KafkaConfigOverrides | None = None`
     and merges it over the derived properties; `from_output_settings` passes
     `settings.config`.
3. `packages/tkati-core/tkati_core/kafka/consumer.py`
   - Widen `__init__`'s `kafka_config` the same way.
   - `from_input_settings` merges `settings.config` over the four derived
     properties.
4. Tests, in `packages/tkati-core/tests/test_producer.py` and
   `test_consumer.py`: the merge reaches librdkafka, asserted by equality on the
   whole config dict so an accidental extra or renamed property fails; a reserved
   key raises at settings parse, once per reason. The unit tests use a capturing
   stub in place of `NativeProducer` / `NativeConsumer` so they need no broker;
   one test builds a real client with a str, an int and a bool property to
   confirm librdkafka accepts what we send, since the stub only checks our own
   merge. `[dlq]` needs no test of its own — it is an `OutputSettings` built by
   the same `build_producer` path as `[output]`.
5. Docs: a **Client configuration** subsection in
   `packages/tkati-core/README.md` next to **Formats** and **Message keys**,
   with the reserved sets, the two error messages, and the three worked examples
   from the Goal — the compression one carrying the `json` / `arrow-batch`
   guidance, the SASL one carrying the build caveat so nobody debugs
   `SCRAM-SHA-256` against the current wheel. Note in `tkati-node-el` and
   `tkati-node-dedup` READMEs that `[output.config]` and `[input.config]` exist,
   pointing at `tkati-core`'s README rather than duplicating the list. CHANGELOG
   entries for all three.
6. Mark F5's configurability half addressed in
   `design-docs/2026-09-27-tkati-core-audit.md`, leaving the row Open for the
   idempotence default, and cross-reference this doc from it.

## Implementation notes (as built)

- The two error messages are assembled from a module-level `_RESERVED_REASONS`
  dict keyed by property, not from the two frozensets, because the *reason* is
  per-property while membership is per-role: `bootstrap.servers` is in both sets
  and has one reason. The sets stay the public statement of policy;
  `_RESERVED_REASONS` is private prose.
- The message is `config: "<key>" <reason>` and does **not** name the block.
  Pydantic already prefixes the location — a bad `[output.config]` inside a
  node's settings reports as `output` → `Value error, config: "bootstrap.servers"
  is set from ...` — so hardcoding `output.` would have printed it twice, and
  `KafkaOutputSettings` cannot know whether it was loaded as `output` or `dlq`.
- `_reject_reserved` is a module-level function called from a
  `@model_validator(mode="after")` on each settings class, rather than an
  `Annotated` validator on the field type, because the reserved set depends on
  which class the field is in.
- No Rust change, as expected: `config_entries`
  (`packages/tkati-core/src/lib.rs:48`) already stringifies values and lowers
  bools, so `_native.pyi`'s `Mapping[str, object]` was already right.

## Verification

187 `tkati-core` tests and 41 across `tkati-node-el` / `tkati-node-dedup` pass,
with `ruff check packages/` and `ty check packages/` clean.

The new tests pin: the producer's config dict by equality with and without a
passthrough; the consumer's by equality including all four derived properties,
so a passthrough that displaced one would fail there as well as at parse time; a
reserved property per reason (`enable.auto.commit`, `group.id`); and librdkafka's
own validation, both directions — it accepts a str, an int and a bool, and
rejects `compresion.type` and `compression.type = "brotli"` with the messages
quoted in Context, which is the test that stands behind not validating property
names ourselves.

Not asserted, because it is not observable through librdkafka: that a Python
`True` reaches it as `true` rather than `True`. librdkafka parses both (it
rejects `yes`, so the parsing is real but case-insensitive), so the lowering in
`config_entries` is covered only by the bool surviving at all. Its one existing
in-tree user, `enable.auto.commit=False`, has worked since the native port.

Also not measured: the compression ratio or throughput effect of any codec on
real traffic. `benchmarks/bench_kafka_json.py` is broker-free and measures the
JSON encode, so it cannot see it.
