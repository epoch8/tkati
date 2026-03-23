# Deployment

The compiled manifest is the single source of truth for all infrastructure. Different **deployment targets** consume the same manifest and provision the same logical pipeline on different backends.

```
pipeline.jsonnet  →  tkati compile  →  manifest.json  →  [deployment target]
```

## Deployment targets

| Target | Command | Use case |
|---|---|---|
| [Docker Compose](04-1-compose.md) | `tkati compose up` | Local development — starts Redpanda and provisions topics in one step |
| [Kubernetes via Terraform](04-2-kubernetes.md) | `tkati terraform` + `terraform apply` | Production — Kafka topics as Terraform resources, nodes as Kubernetes Deployments |

## What every target does

Regardless of backend, a deployment target is responsible for the same two things:

1. **Provision topics** — create Kafka topics with the correct partitions, retention, replication factor, and compaction policy derived from the manifest.
2. **Run nodes** — launch one process per node, passing topic, broker, schema, and user config via the environment variables specified in [Node Configuration Contract](05-node-configuration.md).

The node container image and the env var contract are identical across targets. A node that runs locally in Docker Compose will run unchanged in Kubernetes.
