# CLAUDE.md

## Version control

This repo uses **Jujutsu (`jj`)**, not plain `git` workflows, even though it's git-colocated. Use `jj` commands (`jj st`, `jj diff`, `jj log`, `jj describe`, `jj new`, etc.) instead of `git status`/`git commit`/etc.

- File moves/renames: just `mv` the files — `jj` auto-snapshots the working copy and detects renames by content, no `git mv`/`jj mv` step needed.
- Check `jj st` / `jj diff` to see pending changes instead of `git status`/`git diff`.

## Python workspace (uv)

Use `uv sync --all-packages` when you want the environment to reflect changes across the workspace (e.g. after editing a package's `pyproject.toml` or touching multiple packages). Syncing a single `--package` swaps the active env to just that package's deps and can uninstall tools (`ruff`, `mypy`) needed by other packages.

### Versioning

All packages in this workspace share one version number. When bumping the version of one package's `pyproject.toml`, bump it in every package to match, including any exact-pinned inter-package dependencies (e.g. `tkati-core==X.Y.Z` in `tkati-node-el`'s `dependencies`). Run `uv sync --all-packages` afterward to update `uv.lock`.
