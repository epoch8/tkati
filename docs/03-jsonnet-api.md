# Jsonnet API Reference

`tkati.libsonnet` exposes constructor functions under the `tk` namespace. Each returns a plain Jsonnet object that can be assigned to a local, passed as an argument, or composed into larger structures.

---

## `tk.schema(name, fields)`

Returns a schema object.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Unique identifier for this schema |
| `fields` | array of field objects | yes | Ordered list of fields |

Each field object has:

| Key | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Field name |
| `type` | string | yes | Arrow type string (see below) |
| `nullable` | bool | no | Defaults to `true` |

**Supported Arrow type strings:**

| String | Arrow type |
|---|---|
| `utf8` | `utf8` (variable-length string) |
| `int32`, `int64` | signed integer |
| `float32`, `float64` | floating point |
| `bool` | boolean |
| `date32` | days since epoch |
| `timestamp[ms, UTC]` | millisecond-precision UTC timestamp |
| `binary` | variable-length bytes |
| `list<T>` | list of inner type `T` |

**Output object:**

```jsonnet
{
  _type: 'schema',
  name: <name>,
  fields: <fields>,
}
```

The `_type` field is used internally for validation. It is stripped from the compiled manifest.

---

## `tk.kafka_cluster(name, brokers, defaults={})`

Defines a Redpanda/Kafka cluster. Clusters are referenced by topics to declare which cluster they live on. This drives Terraform provider alias generation and carries topic-level defaults that apply to all topics on the cluster.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Logical name for this cluster, used as the Terraform provider alias |
| `brokers` | array of strings | yes | Bootstrap broker addresses, e.g. `['redpanda-a:9092']` |
| `defaults` | object | no | Default values applied to all topics on this cluster. Defaults to `{}` |

**`defaults` fields:**

| Key | Type | Description |
|---|---|---|
| `replication_factor` | int | Kafka replication factor. Defaults to `3` |
| `partitions` | int | Default partition count for topics that don't specify one. Defaults to `1` |
| `retention` | string | Default retention for topics that don't specify one. Defaults to `'7d'` |

**Output object:**

```jsonnet
{
  _type: 'kafka_cluster',
  name: <name>,
  brokers: <brokers>,
  defaults: <defaults>,
}
```

---

## `tk.kafka_topic(name, schema, cluster, partitions, retention, compacted=false, encoding='json-per-message')`

Returns a Kafka topic object. `partitions` and `retention` fall back to `cluster.defaults` if not specified.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Unique topic name |
| `schema` | schema object | yes | Output of `tk.schema()` |
| `cluster` | cluster object | yes | Output of `tk.kafka_cluster()` |
| `partitions` | int | no | Number of partitions. Falls back to `cluster.defaults.partitions` |
| `retention` | string | no | Retention duration, e.g. `'7d'`, `'24h'`, `'forever'`. Falls back to `cluster.defaults.retention` |
| `compacted` | bool | no | Enable log compaction. Defaults to `false` |
| `encoding` | string | no | Wire encoding for messages. `'arrow-batch'` or `'json-per-message'`. Defaults to `'json-per-message'`. See [Topics, Schema, and Serialization](06-topics-serialization.md) |

**Output object:**

```jsonnet
{
  _type: 'kafka_topic',
  name: <name>,
  schema: <schema object>,
  cluster: <cluster object>,
  partitions: <partitions>,
  retention: <retention>,
  compacted: <compacted>,
  encoding: <encoding>,
}
```

In the compiled manifest, `schema` is replaced by the schema name string and `cluster` is replaced by the cluster name string.

---

## `tk.input(topic, buffer_size=null, timeout=null, group=null)`

Wraps a topic reference with consumer parameters for use in `tk.node(inputs=...)`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `topic` | topic object | yes | The topic to consume |
| `buffer_size` | int | no | Maximum number of records to buffer before processing |
| `timeout` | string | no | Maximum time to wait for a full buffer before processing anyway, e.g. `'5s'`, `'500ms'` |
| `group` | string | no | Kafka consumer group name. Defaults to the node name |

**Output object:**

```jsonnet
{
  _type: 'input',
  topic: <topic object>,
  buffer_size: <buffer_size>,
  timeout: <timeout>,
  group: <group>,
}
```

---

## `tk.output(topic, key=null)`

Wraps a topic reference with producer parameters for use in `tk.node(outputs=...)`.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `topic` | topic object | yes | The topic to produce to |
| `key` | string | no | Field name from the output schema to use as the Kafka message key. Drives partition assignment. If omitted, records are produced without a key |

**Output object:**

```jsonnet
{
  _type: 'output',
  topic: <topic object>,
  key: <key>,
}
```

---

## `tk.node(name, inputs, outputs=[], handler, config={}, deploy={})`

Returns a node object.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Unique node name, used as the Kubernetes Deployment name |
| `inputs` | topic, `tk.input()`, or array of either | yes | Topics this node consumes |
| `outputs` | topic, `tk.output()`, or array of either | no | Topics this node produces. Defaults to `[]` |
| `handler` | string | yes | Python dotted path to the handler function, e.g. `'pipelines.clicks.enrich_batch'` |
| `config` | object | no | Arbitrary key-value pairs passed to the handler at runtime. Defaults to `{}` |
| `deploy` | object | no | Deployment overrides for this node. Defaults to `{}` |

A bare topic object passed to `inputs` or `outputs` is normalized to a `tk.input()` or `tk.output()` with default parameters. A single value (not a list) is normalized to a single-element list. These forms are all equivalent:

```jsonnet
inputs=clicks_raw
inputs=[clicks_raw]
inputs=[tk.input(clicks_raw)]
```

**`deploy` fields:**

| Key | Type | Description |
|---|---|---|
| `image` | string | Container image |
| `replicas` | int | Number of pod replicas. Defaults to `1` |
| `resources` | object | Kubernetes resource requests/limits |
| `env` | array of `{name, value}` | Additional environment variables injected into the container |

**Output object:**

```jsonnet
{
  _type: 'node',
  name: <name>,
  inputs: <array of tk.input() objects>,
  outputs: <array of tk.output() objects>,
  handler: <handler>,
  config: <config>,
  deploy: <deploy>,
}
```

In the compiled manifest, `inputs` and `outputs` each collapse to an array of objects with the topic name and consumer/producer params.

---

## `tk.pipeline(nodes)`

Compiles the manifest. This is the only function that produces output intended for the runtime — all other constructors produce intermediate objects.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `nodes` | array of node objects | yes | All nodes in the pipeline |

**Compilation steps:**

1. Walk every node's `inputs` and `outputs` arrays to collect all referenced topic objects.
2. Collect all clusters referenced by those topics. Deduplicate by `name`.
3. Key each topic as `"cluster_name/topic_name"` — this is the topic's identity in the manifest and in node input/output references. Two topics on different clusters can share a bare name without collision.
4. Collect all schemas referenced by the collected topics. Deduplicate by `name`.
5. Assemble the manifest maps: `schemas`, `kafka_clusters`, `kafka_topics`, `nodes`.

**Output manifest shape:**

```json
{
  "schemas": {
    "<name>": { "name": "...", "fields": [...] }
  },
  "kafka_clusters": {
    "<name>": { "name": "...", "brokers": ["..."] }
  },
  "kafka_topics": {
    "<cluster>/<name>": { "name": "...", "cluster": "<cluster-name>", "schema": "<schema-name>", "partitions": 12, "replication_factor": 3, "retention": "7d", "compacted": false }
  },
  "nodes": {
    "<name>": {
      "name": "...",
      "inputs":  [{ "topic": "<cluster>/<name>", "buffer_size": null, "timeout": null }, ...],
      "outputs": [{ "topic": "<cluster>/<name>", "key": null }, ...],
      "handler": "...",
      "config":  {},
      "deploy":  { "image": "...", "replicas": 1, "resources": {} }
    }
  }
}
```

**Validation errors caught at compile time:**

- Duplicate `cluster/topic` key with differing definition.
- Duplicate schema name with differing definition.
- Node `handler` string is empty.
- `deploy.image` is unresolved on a node.
- A topic's `schema` field is not a `tk.schema()` object.
- A topic's `cluster` field is not a `tk.kafka_cluster()` object.
