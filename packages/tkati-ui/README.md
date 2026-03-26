# tkati-ui

Web UI for visualising tkati pipeline graphs.

## Usage

```sh
tkati ui pipeline.jsonnet --env env/prod.jsonnet
```

Opens a browser with an interactive graph of the pipeline — nodes, topics, and the edges that connect them.

## Options

| Flag | Description |
|---|---|
| `--env FILE` | Jsonnet file passed as the `env` top-level argument |
| `--port N` | Port to listen on (default: 7749) |
| `--no-open` | Start the server without opening a browser tab |
