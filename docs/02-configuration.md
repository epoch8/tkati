# Configuration System

## Two Layers

The configuration system has two layers:

**DX layer** — what the user writes. A list of nodes. Nodes reference topic objects directly; topics reference schema objects directly. There is no separate listing of topics or schemas; everything is derived by walking the node graph.

**Manifest layer** — what the runtime consumes. A flat JSON object with three keyed maps: `schemas`, `topics`, `nodes`. Produced by `tk.pipeline()`, which traverses the node graph, collects all referenced topics and schemas, deduplicates them by name, and assembles the maps.

The user never manually maintains the manifest layer. It is always derived.

## Constructor Functions

`tkati.libsonnet` provides constructors for schemas, clusters, topics, nodes, and pipelines. Objects are referenced directly in Jsonnet — not by string name — so missing references are caught at Jsonnet evaluation time.

### `tk.schema(name, fields)`

Defines an Arrow schema.

```jsonnet
local raw_click = tk.schema('raw_click', fields=[
  { name: 'event_id', type: 'utf8' },
  { name: 'user_id',  type: 'int64' },
  { name: 'url',      type: 'utf8' },
  { name: 'ts',       type: 'timestamp[ms, UTC]' },
]);
```

### `tk.kafka_cluster(name, brokers, defaults={})`

Declares a Redpanda cluster. Carries the broker addresses and optional topic-level defaults.

```jsonnet
local prod_cluster = tk.kafka_cluster('prod',
  brokers=['redpanda-prod:9092'],
  defaults={ replication_factor: 3, retention: '7d' },
);
```

### `tk.kafka_topic(name, schema, cluster, ...)`

Declares a Kafka topic typed to a schema, on a specific cluster.

```jsonnet
local clicks_raw = tk.kafka_topic('clicks_raw',
  schema=raw_click,
  cluster=prod_cluster,
  partitions=12,
);
```

### `tk.node(name, inputs, outputs=[], handler, config={}, deploy={})`

Defines a processing node. `inputs` and `outputs` accept a topic, a `tk.input()`/`tk.output()` wrapper, or a list of either. A bare topic is normalized to a wrapper with default parameters. Pass `inputs=[]` for generator nodes that have no input topics.

```jsonnet
local click_enricher = tk.node('click_enricher',
  inputs=tk.input(clicks_raw, buffer_size=500, timeout='2s'),
  outputs=tk.output(clicks_enriched, key='user_id'),
  handler='pipelines.clicks.enrich_batch',
  config={ geoip_db: '/data/GeoLite2-City.mmdb' },
  deploy={
    image: 'my-registry/pipelines:1.4.2',
    resources: {
      requests: { cpu: '100m', memory: '256Mi' },
      limits:   { cpu: '500m', memory: '1Gi' },
    },
  },
);
```

### `tk.pipeline(nodes)`

Assembles the manifest. Walks all node `inputs` and `outputs`, collects unique topics and clusters (deduplicated by name), collects unique schemas from those topics, and builds the manifest maps.

```jsonnet
tk.pipeline(nodes=[click_enricher, ...])
```

## A Complete Example

```jsonnet
// pipeline.jsonnet
local tk = import 'tkati.libsonnet';

// schemas
local raw_click = tk.schema('raw_click', fields=[
  { name: 'event_id', type: 'utf8' },
  { name: 'user_id',  type: 'int64' },
  { name: 'url',      type: 'utf8' },
  { name: 'ts',       type: 'timestamp[ms, UTC]' },
]);

local enriched_click = tk.schema('enriched_click', fields=[
  { name: 'event_id', type: 'utf8' },
  { name: 'user_id',  type: 'int64' },
  { name: 'url',      type: 'utf8' },
  { name: 'ts',       type: 'timestamp[ms, UTC]' },
  { name: 'country',  type: 'utf8' },
  { name: 'device',   type: 'utf8' },
]);

// cluster
local prod = tk.kafka_cluster('prod',
  brokers=['redpanda-prod:9092'],
  defaults={ replication_factor: 3 },
);

// topics
local clicks_raw = tk.kafka_topic('clicks_raw',
  schema=raw_click,
  cluster=prod,
  partitions=12,
  retention='7d',
);

local clicks_enriched = tk.kafka_topic('clicks_enriched',
  schema=enriched_click,
  cluster=prod,
  partitions=12,
  retention='30d',
);

// deploy config can be defined once and shared across nodes
local default_deploy = {
  image: 'my-registry/pipelines:1.4.2',
  resources: {
    requests: { cpu: '100m', memory: '256Mi' },
    limits:   { cpu: '500m', memory: '512Mi' },
  },
};

// pipeline — only nodes are listed; topics, clusters, and schemas are derived
tk.pipeline(nodes=[
  tk.node('click_enricher',
    inputs=tk.input(clicks_raw, buffer_size=500, timeout='2s'),
    outputs=tk.output(clicks_enriched, key='user_id'),
    handler='pipelines.clicks.enrich_batch',
    config={ geoip_db: '/data/GeoLite2-City.mmdb' },
    deploy=default_deploy,
  ),
  tk.node('click_aggregator',
    inputs=clicks_enriched,
    outputs=clicks_hourly,
    handler='pipelines.clicks.aggregate_hourly',
    deploy=default_deploy,
  ),
])
```

The compiled manifest produced by `tk.pipeline()`:

```json
{
  "schemas": {
    "raw_click":      { "name": "raw_click",      "fields": [...] },
    "enriched_click": { "name": "enriched_click", "fields": [...] }
  },
  "kafka_clusters": {
    "prod": { "name": "prod", "brokers": ["redpanda-prod:9092"] }
  },
  "kafka_topics": {
    "prod/clicks_raw":      { "name": "clicks_raw",      "cluster": "prod", "schema": "raw_click",      "partitions": 12, "replication_factor": 3, "retention": "7d" },
    "prod/clicks_enriched": { "name": "clicks_enriched", "cluster": "prod", "schema": "enriched_click", "partitions": 12, "replication_factor": 3, "retention": "30d" }
  },
  "nodes": {
    "click_enricher": {
      "name": "click_enricher",
      "inputs":  [{ "topic": "prod/clicks_raw",      "buffer_size": null, "timeout": null }],
      "outputs": [{ "topic": "prod/clicks_enriched", "key": null }],
      "handler": "pipelines.clicks.enrich_batch",
      "config":  { "geoip_db": "/data/GeoLite2-City.mmdb" },
      "deploy": {
        "image":    "my-registry/pipelines:1.4.2",
        "replicas": 1,
        "resources": {
          "requests": { "cpu": "100m", "memory": "256Mi" },
          "limits":   { "cpu": "500m", "memory": "512Mi" }
        }
      }
    }
  }
}
```

Note that in the compiled JSON, topic and schema references collapse back to name strings — the runtime uses these to look up definitions within the same manifest.

The corresponding Python handler:

```python
# pipelines/clicks.py
import pyarrow as pa

def enrich_batch(batch: pa.RecordBatch, config: dict) -> pa.RecordBatch:
    # batch conforms to the `raw_click` schema
    country = lookup_country(batch.column('url'), config['geoip_db'])
    device  = detect_device(batch.column('url'))

    return batch.append_column('country', country) \
                .append_column('device',  device)
```

The handler receives one `pa.RecordBatch` per call and returns one. The runtime handles Kafka offset management, batching, serialization, and error handling — the handler contains only business logic.

## Generator Nodes (ZeroToOne)

A `ZeroToOne` node has no input topics and produces records on a timer. The runner calls `generate()` once per tick; return `None` to skip producing for that tick.

```python
# pipelines/clicks.py
import pyarrow as pa
from tkati_node import ZeroToOne

class ClickGenerator(ZeroToOne):
    def __init__(self, config: dict) -> None:
        self.batch_size = int(config.get("batch_size", 100))

    def generate(self) -> pa.RecordBatch:
        ...  # return a RecordBatch conforming to the output topic schema
```

Declare it in the pipeline with `inputs=[]`:

```jsonnet
tk.node('click_generator',
  inputs=[],
  outputs=tk.output(clicks_raw, key='user_id'),
  handler='pipelines.clicks.ClickGenerator',
  config={ batch_size: 50 },
  deploy=default_deploy,
)
```

The tick interval defaults to 1000 ms and can be overridden with the `TICK_INTERVAL_MS` environment variable.

## Manifest Compilation

Running `tkati compile pipeline.jsonnet` evaluates the Jsonnet, runs `tk.pipeline()`, and validates the result:

- Every topic name is unique across the collected set.
- Every schema name is unique across the collected set.
- Every handler must be importable in the target Python environment.

Validation failures are reported before any infrastructure changes are made.

## Composition

Each sub-file defines schemas, topics, and nodes as locals, then exports just an array of nodes:

```jsonnet
// sub/clicks.jsonnet
local tk = import '../tkati.libsonnet';

local prod           = tk.kafka_cluster('prod', brokers=['redpanda-prod:9092']);
local raw_click      = tk.schema('raw_click', [...]);
local enriched_click = tk.schema('enriched_click', [...]);
local clicks_raw      = tk.kafka_topic('clicks_raw',      schema=raw_click,      cluster=prod, ...);
local clicks_enriched = tk.kafka_topic('clicks_enriched', schema=enriched_click, cluster=prod, ...);

{
  nodes: [
    tk.node('click_enricher',
      inputs=[clicks_raw],
      outputs=[clicks_enriched],
      handler='pipelines.clicks.enrich_batch',
    ),
  ],
}
```

The root file assembles sub-pipelines by concatenating their node arrays:

```jsonnet
// pipeline.jsonnet
local df       = import 'tkati.libsonnet';
local clicks   = import 'sub/clicks.jsonnet';
local payments = import 'sub/payments.jsonnet';

tk.pipeline(nodes=clicks.nodes + payments.nodes)
```

Topics and schemas defined in separate sub-files but referencing the same underlying object (imported from a shared library) are deduplicated automatically by name.
