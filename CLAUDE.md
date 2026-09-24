# CLAUDE.md

## Version control

This repo uses **Jujutsu (`jj`)**, not plain `git` workflows, even though it's git-colocated. Use `jj` commands (`jj st`, `jj diff`, `jj log`, `jj describe`, `jj new`, etc.) instead of `git status`/`git commit`/etc.

- File moves/renames: just `mv` the files — `jj` auto-snapshots the working copy and detects renames by content, no `git mv`/`jj mv` step needed.
- Check `jj st` / `jj diff` to see pending changes instead of `git status`/`git diff`.

## Python workspace (uv)

Use `uv sync --all-packages` when you want the environment to reflect changes across the workspace (e.g. after editing a package's `pyproject.toml` or touching multiple packages). Syncing a single `--package` swaps the active env to just that package's deps and can uninstall tools (`ruff`, `mypy`) needed by other packages.

### Type checking

Types are checked with **`ty`** (Astral's type checker), not mypy or pyright. Run `uv run ty check packages/` — or `uv run ty check packages/<name>` for one package. Each package's `test-*.yml` workflow runs it alongside `ruff check` and `pytest`, so it gates CI. It's also what the IDE's inline diagnostics come from, so a clean `ty check` means a clean editor.

Note that `ty` reads third-party `.pyi` stubs as ground truth, and some are wrong. When a stub disagrees with runtime behavior, prefer a narrow `cast("Any", obj)` at the call site with a comment naming the runtime behavior, rather than reshaping the code to satisfy the stub.

### Versioning

All packages in this workspace share one version number, **and so does the workspace root**. A version bump means changing all of these to the same value:

- the root `pyproject.toml`'s `version` (the `tkati` workspace project itself — easy to miss, since nothing depends on it)
- every `packages/*/pyproject.toml`'s `version`
- every exact-pinned inter-package dependency (`tkati-core==X.Y.Z` appears in `tkati-node-el`, `tkati-node-dedup` and `tkati-dashboard`)
- `packages/tkati-core/Cargo.toml`'s `version` (the Rust crate behind `tkati_core._native`; maturin takes the wheel version from `pyproject.toml`, but keep them equal)

Run `uv sync --all-packages` afterward to update `uv.lock`. To check nothing was missed, `grep -h '^version' pyproject.toml packages/*/pyproject.toml packages/tkati-core/Cargo.toml | sort -u` should print exactly one line.

## Rust extension (tkati-core)

`tkati-core` is built with **maturin**: `packages/tkati-core/src/` is a pyo3 crate compiled to `tkati_core._native` (librdkafka statically linked, JSON encoding parallelised with rayon). `KafkaConsumer`/`KafkaProducer` are thin Python wrappers over it.

- `uv sync --all-packages` rebuilds the extension whenever `src/**/*.rs`, `Cargo.toml` or `Cargo.lock` change (see `[tool.uv] cache-keys`). The first build compiles librdkafka from source and takes a couple of minutes.
- Rust checks, run in `packages/tkati-core`: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`. CI runs them in `test-tkati-core.yml`.
- `tkati_core/_native.pyi` is the hand-written stub `ty` checks against. Update it with any change to the native API.
- Codec performance: `uv run python packages/tkati-core/benchmarks/bench_kafka_json.py` (broker-free; compares against the pre-native implementation).
- Which packages get the maturin CI variant is the `maturin` list at the top of `.github/workflow-templates/{test,publish}.template.yml`. The variant is kept inside those templates, not in separate template types, because the generated filename is `<template_type>-<package>.yml` and PyPI trusted publishing is pinned to `publish-tkati-core.yml`.
