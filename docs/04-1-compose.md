# Deployment: Docker Compose

The Docker Compose target is for local development. It starts a Redpanda instance for every Kafka cluster declared in the pipeline, waits for it to become healthy, and provisions all topics — in a single command.

```
pipeline.jsonnet  →  tkati compose up  →  Redpanda running, topics created
```

## Usage

```sh
tkati compose up pipeline.jsonnet --env env/dev.jsonnet
```

The command:

1. Compiles the pipeline manifest.
2. Writes a `docker-compose.yml` to `.tkati/docker-compose.yml`.
3. Runs `docker compose up -d`.
4. Polls the Redpanda admin endpoint until healthy.
5. Creates all topics via the Kafka admin API.

## Generated docker-compose.yml

One `redpanda-<cluster>` service is generated per cluster. Each service runs in `--mode dev-container` (single node, no ZooKeeper) with two listeners:

| Listener | Address | Used by |
|---|---|---|
| internal | `redpanda-<cluster>:9092` | Other containers on the same compose network |
| external | `localhost:19092` | Topic provisioning and local Kafka clients from the host |

Port assignment for multiple clusters: the first cluster gets `19092` / `9644`, the second `19093` / `9645`, and so on.

```yaml
services:
  redpanda-prod:
    image: docker.redpanda.com/redpandadata/redpanda:latest
    command:
      - redpanda
      - start
      - --kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr=internal://redpanda-prod:9092,external://localhost:19092
      - --mode=dev-container
      - --smp=1
      - --default-log-level=warn
    ports:
      - "19092:19092"
      - "9644:9644"
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:\\s+true'"]
      interval: 3s
      timeout: 5s
      retries: 20
```

## Topic provisioning

Topics are created via the Kafka admin API once Redpanda is healthy. The manifest fields map to Kafka topic configuration as follows:

| Manifest field | Kafka config |
|---|---|
| `partitions` | `num_partitions` |
| `retention` | `retention.ms` (converted from human-readable, e.g. `'7d'` → `604800000`) |
| `retention: 'forever'` | `retention.ms = -1` |
| `compacted: true` | `cleanup.policy = compact` |

`replication_factor` is fixed at `1` — Redpanda in dev-container mode runs as a single node.

Already-existing topics are left unchanged.

## Options

| Flag | Default | Description |
|---|---|---|
| `--env FILE` | — | Environment TLA file passed to the pipeline function |
| `--compose-file FILE` | `.tkati/docker-compose.yml` | Where to write the generated compose file |
| `--timeout SECONDS` | `60` | How long to wait for Redpanda before giving up |

## Inspecting and managing the environment

The generated compose file is written to `.tkati/docker-compose.yml` so you can use `docker compose` directly:

```sh
# view logs
docker compose -f .tkati/docker-compose.yml logs -f

# stop everything
docker compose -f .tkati/docker-compose.yml down

# list topics
docker compose -f .tkati/docker-compose.yml exec redpanda-prod rpk topic list
```
