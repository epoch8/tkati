# tkati-compose

Local Docker Compose environment for tkati pipelines.

## Usage

```sh
tkati compose up pipeline.jsonnet --env env/dev.jsonnet
```

Starts a Redpanda instance for each Kafka cluster declared in the pipeline, waits for it to be ready, then provisions all topics with the correct partitions, retention, and compaction settings.

The generated `docker-compose.yml` is written to `.tkati/docker-compose.yml` so you can inspect it or run `docker compose` commands against it directly.
