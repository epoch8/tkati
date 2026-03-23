# Deployment: Kubernetes via Terraform

The Kubernetes target is for production. `tkati terraform` converts the compiled manifest into a Terraform JSON file that provisions Kafka topics and Kubernetes Deployments in one `terraform apply`.

```
pipeline.jsonnet  →  tkati terraform  →  main.tf.json  →  terraform apply
```

## Usage

```sh
tkati terraform pipeline.jsonnet --env env/prod.jsonnet --out infra/main.tf.json
cd infra && terraform apply
```

## Topics → Kafka topic resources

Each topic becomes a `kafka_topic` resource using the [Mongey/kafka](https://registry.terraform.io/providers/Mongey/kafka/latest/docs) provider.

| Manifest field | Terraform / Kafka config |
|---|---|
| `partitions` | `partitions` |
| `replication_factor` | `replication_factor` (falls back to `cluster.defaults.replication_factor`, then `3`) |
| `retention` | `config.retention.ms` (converted from human-readable) |
| `retention: 'forever'` | `config.retention.ms = "-1"` |
| `compacted: true` | `config.cleanup.policy = "compact"` |

```json
{
  "resource": {
    "kafka_topic": {
      "prod__clicks_raw": {
        "provider": "kafka.prod",
        "name": "clicks_raw",
        "partitions": 12,
        "replication_factor": 3,
        "config": {
          "retention.ms": "604800000",
          "cleanup.policy": "delete"
        }
      }
    }
  }
}
```

### Provider configuration

Topics reference their cluster via the Terraform `provider` field, using the cluster name as the alias. Provider configuration is **not** generated — write it manually in a `providers.tf` alongside the generated file. This keeps broker addresses and credentials out of generated artifacts.

```hcl
# providers.tf — written by hand, not generated
provider "kafka" {
  alias             = "prod"
  bootstrap_servers = ["redpanda-prod:9092"]
}
```

The alias must match `tk.kafka_cluster(name, ...)` exactly.

## Nodes → Kubernetes Deployments

Each node becomes a `kubernetes_deployment`. The node's `handler`, `config`, and `deploy` block map to the container spec.

The handler is the container command:

```
tkati-node pipelines.clicks.enrich_batch
```

Topic, broker, schema, and user config are passed as environment variables per the [Node Configuration Contract](05-node-configuration.md). Broker addresses are resolved from the cluster definition — the node never handles cluster names.

```json
{
  "resource": {
    "kubernetes_deployment": {
      "click_enricher": {
        "metadata": [{ "name": "click-enricher", "namespace": "pipelines" }],
        "spec": [{
          "replicas": 1,
          "selector": [{ "match_labels": { "app": "click-enricher" } }],
          "template": [{
            "metadata": [{ "labels": { "app": "click-enricher" } }],
            "spec": [{
              "container": [{
                "name": "node",
                "image": "my-registry/pipelines:1.4.2",
                "command": ["tkati-node", "pipelines.clicks.enrich_batch"],
                "env": [
                  { "name": "INPUT__TOPIC",       "value": "clicks_raw" },
                  { "name": "INPUT__BROKERS",     "value": "redpanda-prod:9092" },
                  { "name": "INPUT__DATA_SCHEMA",      "value": "[{\"name\":\"event_id\",\"type\":\"utf8\"},...]" },
                  { "name": "INPUT__GROUP",       "value": "click_enricher" },
                  { "name": "OUTPUT__TOPIC",      "value": "clicks_enriched" },
                  { "name": "OUTPUT__BROKERS",    "value": "redpanda-prod:9092" },
                  { "name": "OUTPUT__DATA_SCHEMA",     "value": "[{\"name\":\"event_id\",\"type\":\"utf8\"},...]" },
                  { "name": "OUTPUT__KEY",        "value": "user_id" },
                  { "name": "TKATI_CONFIG",            "value": "{\"geoip_db\":\"/data/GeoLite2-City.mmdb\"}" }
                ],
                "resources": [{
                  "requests": { "cpu": "100m", "memory": "256Mi" },
                  "limits":   { "cpu": "500m", "memory": "512Mi" }
                }]
              }]
            }]
          }]
        }]
      }
    }
  }
}
```

Node names are converted from `snake_case` to `kebab-case` for Kubernetes resource names (`click_enricher` → `click-enricher`).

Any `env` entries from the node's `deploy` block are appended after the runtime-managed variables.

## Incremental updates

Resources are named after their manifest keys. Re-running `tkati terraform` on a changed pipeline produces a diff Terraform can apply incrementally — adding new topics/nodes, modifying changed ones, destroying removed ones.

Renaming a topic or node destroys the old resource and creates a new one. For topics this means data loss — rename with care.
