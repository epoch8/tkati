# tkati-core

Jsonnet library for declaring data pipelines as directed graphs of Kafka topics and Python processing nodes.

## What it provides

`tkati.libsonnet` — constructor functions for building typed pipeline definitions:

| Constructor | Purpose |
|---|---|
| `tk.schema(name, fields)` | Define an Arrow data schema |
| `tk.kafka_cluster(name, brokers, defaults)` | Declare a Redpanda/Kafka cluster |
| `tk.kafka_topic(name, schema, cluster, ...)` | Declare a typed topic |
| `tk.input(topic, ...)` / `tk.output(topic, ...)` | Wrap a topic with node-side parameters |
| `tk.node(name, inputs, outputs, handler, ...)` | Define a processing node (→ one K8s Deployment) |
| `tk.pipeline(nodes)` | Assemble a manifest from a list of nodes |

`tkati-terraform.libsonnet` — converts a compiled manifest to Terraform JSON (Kafka topics + K8s Deployments).

## Usage

```jsonnet
// pipeline.jsonnet
local tk = import 'tkati.libsonnet';

local raw_click = tk.schema('raw_click', fields=[
  { name: 'user_id', type: 'int64' },
  { name: 'url',     type: 'utf8' },
  { name: 'ts',      type: 'timestamp[ms, UTC]' },
]);

local prod = tk.kafka_cluster('prod', brokers=['redpanda-prod:9092']);

local clicks_raw = tk.kafka_topic('clicks_raw',
  schema=raw_click, cluster=prod, partitions=12);

function(env) tk.pipeline(nodes=[
  tk.node('click_enricher',
    inputs=tk.input(clicks_raw, buffer_size=500, timeout='2s'),
    outputs=tk.output(tk.kafka_topic('clicks_enriched', ...)),
    handler='pipelines.clicks.enrich_batch',
  ),
])
```

Compile to a manifest with [`tkati-cli`](../tkati-cli/README.md):

```sh
tkati compile pipeline.jsonnet --env env/prod.jsonnet
```
