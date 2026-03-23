# Node Configuration Contract

When a `tkati-node` process starts, it receives everything it needs through environment variables injected by the Kubernetes Deployment.

## Single input / single output (common case)

The default — and by far the most common — case uses `INPUT__*` / `OUTPUT__*` variables. The double underscore is the nested-field delimiter used by pydantic-settings.

```
INPUT__TOPIC=clicks_raw
INPUT__BROKERS=redpanda-prod:9092
INPUT__DATA_SCHEMA=[{"name":"event_id","type":"utf8"},{"name":"user_id","type":"int64"},{"name":"url","type":"utf8"},{"name":"ts","type":"timestamp[ms, UTC]"}]
INPUT__ENCODING=json-per-message
INPUT__GROUP=click_enricher
INPUT__BUFFER_SIZE=500
INPUT__TIMEOUT=2s

OUTPUT__TOPIC=clicks_enriched
OUTPUT__BROKERS=redpanda-prod:9092
OUTPUT__DATA_SCHEMA=[{"name":"event_id","type":"utf8"},{"name":"user_id","type":"int64"},{"name":"url","type":"utf8"},{"name":"ts","type":"timestamp[ms, UTC]"},{"name":"country","type":"utf8"},{"name":"device","type":"utf8"}]
OUTPUT__ENCODING=arrow-batch
OUTPUT__KEY=user_id

TKATI_CONFIG={"geoip_db":"/data/GeoLite2-City.mmdb"}
```

### Input variables

| Variable | Source | Description |
|---|---|---|
| `INPUT__TOPIC` | `kafka_topics[*].name` | Bare Kafka topic name — no cluster prefix |
| `INPUT__BROKERS` | `kafka_clusters[cluster].brokers` | Comma-separated bootstrap broker addresses |
| `INPUT__DATA_SCHEMA` | `schemas[topic.schema]` | Full Arrow schema as JSON — used to deserialize incoming messages |
| `INPUT__ENCODING` | `kafka_topics[*].encoding` | Wire encoding: `arrow-batch` or `json-per-message`. See [Topics, Schema, and Serialization](06-topics-serialization.md) |
| `INPUT__GROUP` | node name (default) or `tk.input(group=...)` | Kafka consumer group name |
| `INPUT__BUFFER_SIZE` | `tk.input(buffer_size=...)` | Max records to buffer before processing. Omitted if not set → runtime default |
| `INPUT__TIMEOUT` | `tk.input(timeout=...)` | Max wait before flushing a partial buffer. Omitted if not set → runtime default |

### Output variables

| Variable | Source | Description |
|---|---|---|
| `OUTPUT__TOPIC` | `kafka_topics[*].name` | Bare Kafka topic name — no cluster prefix |
| `OUTPUT__BROKERS` | `kafka_clusters[cluster].brokers` | Comma-separated bootstrap broker addresses |
| `OUTPUT__DATA_SCHEMA` | `schemas[topic.schema]` | Full Arrow schema as JSON — used to validate and serialize outgoing record batches |
| `OUTPUT__ENCODING` | `kafka_topics[*].encoding` | Wire encoding: `arrow-batch` or `json-per-message`. See [Topics, Schema, and Serialization](06-topics-serialization.md) |
| `OUTPUT__KEY` | `tk.output(key=...)` | Field from the output schema to use as the Kafka message key. Omitted → no key |

### Handler

| Variable | Description |
|---|---|
| `TKATI_CONFIG` | User config from `tk.node(config=...)`, serialized as JSON, passed to the handler as `config: dict` |

---

## Multiple inputs or outputs (rare)

When a node declares more than one input or output, `INPUTS` and `OUTPUTS` each hold a JSON-encoded array. Each element uses the same field names as the single-input form:

```
INPUTS=[
  {"topic":"clicks_raw","brokers":"redpanda-prod:9092","data_schema":"{...}","encoding":"json-per-message","group":"click_enricher","buffer_size":500,"timeout":"2s"},
  {"topic":"user_profiles","brokers":"redpanda-analytics:9092","data_schema":"{...}","encoding":"arrow-batch","group":"click_enricher"}
]
OUTPUTS=[
  {"topic":"clicks_enriched","brokers":"redpanda-prod:9092","data_schema":"{...}","encoding":"arrow-batch","key":"user_id"}
]
```

The array order matches the order of `inputs` / `outputs` in the manifest. Optional fields (`buffer_size`, `timeout`, `key`) may be omitted.

---

## Consumer group name

The consumer group identifies this node's position in a topic's offset log. The default is the node name — predictable, stable, and unique per pipeline node:

```
INPUT__GROUP=click_enricher
```

Override it with `tk.input(group=...)` when the same logical consumer group must span multiple deployments or when migrating a node without resetting offsets.

---

## Multi-input handler signature

For multi-input nodes the handler receives a `source` argument identifying which input topic produced the batch:

```python
def process(self, batch: pa.RecordBatch, source: str) -> ...:
    ...
```

Returning `None` signals that the batch was consumed for internal state (e.g. building an in-memory lookup table) and produces no output on this call.
