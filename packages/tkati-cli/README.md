# tkati-cli

CLI for compiling [tkati](../tkati-core/README.md) pipeline definitions to manifests and Terraform.

## Installation

```sh
uv tool install tkati-cli
```

## Commands

### `tkati compile`

Evaluate a `pipeline.jsonnet` and print the compiled JSON manifest to stdout.

```sh
tkati compile pipeline.jsonnet
tkati compile pipeline.jsonnet --env env/prod.jsonnet
tkati compile pipeline.jsonnet --env env/prod.jsonnet --out manifest.json
```

### `tkati terraform`

Generate Terraform JSON (Kafka topics + Kubernetes Deployments) from a pipeline.

```sh
tkati terraform pipeline.jsonnet --env env/prod.jsonnet
tkati terraform pipeline.jsonnet --env env/prod.jsonnet --out infra/main.tf.json
```

## Options

| Flag | Description |
|---|---|
| `--env FILE` | Jsonnet file passed as the `env` top-level argument to the pipeline function |
| `--out FILE` | Write output to a file instead of stdout |
