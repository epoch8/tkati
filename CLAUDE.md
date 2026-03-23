# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Tkati** is a declarative data pipeline orchestration framework. Pipelines are defined in Jsonnet, compiled to a JSON manifest, and deployed as Kafka-connected micro-services (nodes) on Kubernetes. A local Docker Compose environment supports development.

## Commands

### Python (UV workspace)

```bash
# Install all packages in editable mode
uv sync

# Run tests
uv run pytest

# Run a specific test
uv run pytest packages/tkati-node/tests/test_runner.py::test_name

# Run the CLI
uv run tkati compile examples/clicks/pipeline.jsonnet
uv run tkati terraform examples/clicks/pipeline.jsonnet
uv run tkati compose up examples/clicks/pipeline.jsonnet
uv run tkati ui examples/clicks/pipeline.jsonnet
```

### Frontend (tkati-ui)

```bash
cd packages/tkati-ui/frontend
npm install
npm run dev      # Vite dev server
npm run build    # TypeScript check + Vite build
```

## Architecture

### Compilation Pipeline

Jsonnet → JSON manifest → (Terraform JSON | Docker Compose | UI visualization)

All downstream tools consume the same compiled manifest (`tkati compile` output). The manifest contains schemas, Kafka clusters, topics, and processing nodes.

### Packages

- **tkati-core** — Jsonnet library (`tkati.libsonnet`) with constructors: `tk.schema`, `tk.kafka_cluster`, `tk.kafka_topic`, `tk.input`, `tk.output`, `tk.node`, `tk.pipeline`. Also contains `tkati-terraform.libsonnet` for Terraform generation.
- **tkati-cli** — Click CLI (`tkati` command). Entry point: `tkati_cli.cli:main`. Subcommands: `compile`, `terraform`, `ui` (delegated to tkati-ui), `compose` (delegated to tkati-compose).
- **tkati-node** — Runtime SDK for individual processing nodes. Nodes are started via `tkati-node <dotted.handler.Path>`. Base classes in `base.py`: `OneToOne`, `OneToMany`, `ManyToMany`, `ZeroToOne`.
- **tkati-compose** — Generates and manages a Docker Compose environment: starts Redpanda containers, provisions topics, runs node services.
- **tkati-ui** — FastAPI server (`GET /api/manifest`) + React/TypeScript SPA that renders the pipeline as an interactive DAG (XYFlow + Dagre layout).

### Node Runtime (tkati-node)

Nodes are stateless micro-services:

1. Configuration is loaded from environment variables (Pydantic settings in `env.py`)
2. `runner.py` polls Kafka input topics, accumulates micro-batches (configurable size/timeout)
3. Batches are deserialized to Apache Arrow `RecordBatch` via `serde.py`
4. User-defined handler is called with the batch; returns a batch or dict of batches
5. Output batches are serialized and produced to output Kafka topics
6. Prometheus metrics are exported: `tkati_records_consumed_total`, `tkati_records_produced_total`, `tkati_handler_duration_seconds`, `tkati_consumer_lag_records`, `tkati_handler_errors_total`

### Data Flow

```
Jsonnet pipeline definition
        ↓ tkati compile
    JSON manifest
   ↙      ↓       ↘
Terraform  Docker   tkati-ui
           Compose  (visualization)
                ↓
         Kafka topics + Node deployments
                ↓
         tkati-node runtime (per node)
```

### Example Pipeline

See [examples/clicks/](examples/clicks/) for a working example with Jsonnet definitions and Python handler implementations.

## Key Conventions

- **Python 3.13** required (see `.python-version`)
- **UV** is the package manager; use `uv run` to execute commands in the workspace
- Compile artifacts go to `.tkati/` (gitignored); Docker Compose file is `.tkati/docker-compose.yml`
- Node handlers extend one of the four base classes and override `process()` or `generate()`
- Arrow IPC is the default serialization format between Kafka and node handlers; JSON-per-message is a fallback
