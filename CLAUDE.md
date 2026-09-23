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

Run `uv sync --all-packages` afterward to update `uv.lock`. To check nothing was missed, `grep -h '^version' pyproject.toml packages/*/pyproject.toml | sort -u` should print exactly one line.
