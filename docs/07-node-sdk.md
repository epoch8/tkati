# Node SDK

The Node SDK (`tkati-node`) is the runtime that executes a pipeline node. It handles everything below the business logic layer: Kafka polling, batching, serialisation, offset commits, and error handling. The node author writes a class; the SDK instantiates and runs it.

## Base classes

The SDK provides three base classes covering the common input/output configurations. Import them from `tkati.node`:

```python
from tkati_node import OneToOne, OneToMany, ManyToMany
```

| Base class | Inputs | Outputs | `process` signature |
|---|---|---|---|
| `OneToOne` | 1 | 1 | `process(self, batch)` |
| `OneToMany` | 1 | N | `process(self, batch) -> dict[str, RecordBatch]` |
| `ManyToMany` | N | N | `process(self, batch, source) -> dict[str, RecordBatch] \| None` |

Schemas are not declared in the class. The SDK reads them from the environment at startup and uses them for serialisation and validation transparently. The class only processes `pa.RecordBatch` objects.

---

## `OneToOne`

Single input topic, single output topic. Override `process`.

```python
import pyarrow as pa
from tkati_node import OneToOne

class ClickEnricher(OneToOne):

    def __init__(self, config: dict) -> None:
        self.geoip_db = config['geoip_db']

    def process(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        country = lookup_country(batch.column('url'), self.geoip_db)
        device  = detect_device(batch.column('url'))
        return batch.append_column('country', country) \
                    .append_column('device',  device)
```

`__init__` receives the `config` dict from `tk.node(config=...)` and is called once at startup. Use it for any one-time initialisation — loading models, opening connection pools, reading lookup files.

Returning `None` from `process` is valid for sink nodes with no output topic.

---

## `OneToMany`

Single input topic, multiple output topics. Override `process` and return a `dict` keyed by output topic name.

```python
from tkati_node import OneToMany

class ClickRouter(OneToMany):

    def process(self, batch: pa.RecordBatch) -> dict[str, pa.RecordBatch]:
        mask_mobile  = compute_mask(batch, 'device', 'mobile')
        mask_desktop = compute_mask(batch, 'device', 'desktop')
        return {
            'clicks_mobile':  batch.filter(mask_mobile),
            'clicks_desktop': batch.filter(mask_desktop),
        }
```

Keys must match the bare output topic names declared in `tk.node(outputs=...)`. Topics absent from the returned dict receive no records for that batch.

---

## `ManyToMany`

Multiple input topics, multiple output topics. Override `process` with the additional `source` argument — the bare input topic name. Return a `dict` keyed by output topic name, or `None` to produce no output for that batch.

```python
from tkati_node import ManyToMany

class ClickJoiner(ManyToMany):

    def __init__(self, config: dict) -> None:
        self._profiles: dict[str, dict] = {}

    def process(
        self, batch: pa.RecordBatch, source: str
    ) -> dict[str, pa.RecordBatch] | None:
        if source == 'user_profiles':
            for row in batch.to_pylist():
                self._profiles[row['user_id']] = row
            return None   # accumulated into cache, no output yet
        # source == 'clicks_raw' — enrich and route by device type
        enriched = join_with_profiles(batch, self._profiles)
        return {
            'clicks_mobile':  enriched.filter(is_mobile(enriched)),
            'clicks_desktop': enriched.filter(is_desktop(enriched)),
        }
```

Returning `None` produces no output for that batch — the typical pattern for the accumulation side of a join.

---

## Class discovery

The class is located at startup using the dotted path from `tk.node(handler=...)`:

```
pipelines.clicks.ClickEnricher
  └─ module:  pipelines.clicks
  └─ class:   ClickEnricher
```

The module is imported once, the class is instantiated once (with the `config` dict), and the same instance handles every batch for the lifetime of the process.

---

## Batching

The SDK accumulates incoming records into batches before calling the node. Two conditions trigger a call:

- **`buffer_size`** — the batch has reached the configured number of records.
- **`timeout`** — the configured duration has elapsed since the first record arrived in the current batch.

Whichever fires first wins. At low throughput the timeout is the primary trigger; at high throughput the buffer fills before the timeout.

Both are configured per input topic via `tk.input(buffer_size=..., timeout=...)`. For `json-per-message` topics, `buffer_size` counts individual Kafka messages (rows). For `arrow-batch` topics, it counts rows within the deserialised batch.

---

## Offset management

Offsets are committed only after the node method returns successfully **and** any output has been produced to all output topics. A crash before that point leaves the offset uncommitted — the batch replays in full on the next startup.

This gives at-least-once delivery. Node logic should be idempotent, or outputs should be deduplicated downstream.

---

## Error handling

If a node method raises an unhandled exception the SDK logs the error with full traceback and exits with a non-zero status. Kubernetes restarts the pod, which replays the uncommitted batch from the last committed offset.

The SDK does not retry locally. Crash-and-restart provides a clear signal, a natural backoff via the Kubernetes restart policy, and a consistent recovery path.

If a batch consistently causes a crash (a poison message), the pod enters CrashLoopBackOff — intentional, as it surfaces the problem rather than silently skipping records.

---

## Serialisation

The SDK reads `INPUT_KAFKA_ENCODING` and `OUTPUT_KAFKA_ENCODING` from the environment and handles serialisation transparently. Node methods always operate on `pa.RecordBatch` regardless of the wire encoding.

See [Topics, Schema, and Serialization](06-topics-serialization.md) for the full encoding reference.

---

## Packaging

The node container image must contain both:

1. **`tkati-node`** — the SDK entry point.
2. **The node module** — the Python package containing the node class.

A minimal `Dockerfile`:

```dockerfile
FROM python:3.13-slim
RUN pip install tkati-node
COPY pipelines/ /app/pipelines/
RUN pip install /app
WORKDIR /app
```

The `image` field in `tk.node(deploy={image: ...})` must point to this image.

---

## Observability

The SDK exposes a Prometheus metrics endpoint on port `9090` (`/metrics`):

| Metric | Type | Description |
|---|---|---|
| `tkati_records_consumed_total` | counter | Records consumed from input topics |
| `tkati_records_produced_total` | counter | Records produced to output topics |
| `tkati_handler_duration_seconds` | histogram | Node method call latency (p50, p95, p99) |
| `tkati_consumer_lag_records` | gauge | Current consumer lag per input topic |
| `tkati_handler_errors_total` | counter | Number of unhandled exceptions (just before exit) |
