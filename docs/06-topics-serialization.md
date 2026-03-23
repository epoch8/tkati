# Topics, Schema, and Serialization

## Topics as typed edges

Kafka topics are the edges of the pipeline graph. Each topic carries records of exactly one schema — the schema is declared once in the pipeline definition and enforced by the SDK on every read and write.

Encoding is a property of the topic, not the node. A topic has one encoding; all producers and consumers of that topic must honour it. Node authors do not choose an encoding — the SDK reads it from the environment and handles both sides automatically.

## Encoding options

### `arrow-batch`

One Kafka message carries one Arrow `RecordBatch` serialised in Arrow IPC stream format. The schema is embedded in the IPC bytes — messages are self-describing.

**Use when** the topic is consumed and produced exclusively by tkati nodes.

```jsonnet
local clicks_enriched = tk.kafka_topic('clicks_enriched',
  schema=enriched_click,
  cluster=prod,
  encoding='arrow-batch',
);
```

Characteristics:
- Efficient: columnar layout, zero-copy deserialisation into Arrow
- One message per batch — typically hundreds to thousands of rows
- Not human-readable; requires Arrow tooling to inspect
- Schema mismatch is detected immediately at deserialisation time

### `json-per-message` (default)

One Kafka message carries one JSON object representing one row. The SDK collects incoming messages into batches before calling the handler, and explodes outgoing batches into individual messages on produce.

**Use when** the topic is shared with external producers or consumers (web services, other frameworks, ad-hoc consumers). This is the default — it favours interoperability and inspectability over efficiency, and is the safer choice when a topic's consumers are not fully known upfront.

```jsonnet
local clicks_raw = tk.kafka_topic('clicks_raw',
  schema=raw_click,
  cluster=prod,
  // encoding='json-per-message' is the default — can be omitted
);
```

Characteristics:
- Universally interoperable — any Kafka client can read and write the topic
- Human-readable; messages are inspectable with standard tools
- One Kafka message per row — higher overhead at large batch sizes
- The SDK uses the schema for type coercion and validation during JSON parsing

## How the SDK handles each encoding

| Aspect | `arrow-batch` | `json-per-message` |
|---|---|---|
| **Consume** | Deserialise IPC bytes → `RecordBatch`, validate schema | Accumulate messages up to `buffer_size` or `timeout`, parse JSON, coerce types, validate schema → `RecordBatch` |
| **Produce** | Serialise `RecordBatch` → IPC bytes, one Kafka message | Explode `RecordBatch` into rows, serialise each as JSON, produce N Kafka messages |
| **Schema use** | Validation after deserialise | Type coercion + validation during JSON parse |
| **Message key** | Extracted from batch column after serialise | Extracted from row field before produce |

## Schema validation

The SDK validates every batch against the declared schema before passing it to the handler (on input) and before producing it (on output). A mismatch is a hard error — the SDK does not silently drop or coerce unexpected fields.

For `json-per-message` inputs, the SDK applies the following type coercions during JSON parse:

| Arrow type | Accepted JSON |
|---|---|
| `utf8` | string |
| `int32`, `int64` | number (truncated to integer) |
| `float32`, `float64` | number |
| `bool` | boolean |
| `timestamp[ms, UTC]` | ISO 8601 string or integer milliseconds since epoch |
| `date32` | ISO 8601 date string or integer days since epoch |
| `binary` | base64-encoded string |

Fields present in the JSON message but absent from the schema are dropped. Fields declared in the schema but absent from the JSON message are filled with `null` (schema permitting) or raise a validation error.

## Choosing an encoding

| Topic role | Recommended encoding |
|---|---|
| Ingested from external system (web events, CDC, webhooks) | `json-per-message` (default) |
| Exported to external system (data warehouse loader, alerting) | `json-per-message` (default) |
| Internal intermediate topic between tkati nodes only | `arrow-batch` |
| Compacted state topic (internal) | `arrow-batch` |
