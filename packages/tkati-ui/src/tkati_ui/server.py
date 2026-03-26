from __future__ import annotations

import importlib.resources
import json
from pathlib import Path

import _jsonnet
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

_pipeline_file: str = ""
_env_file: str | None = None

_static_dir = Path(__file__).parent / "static"


def configure(pipeline_file: str, env_file: str | None = None) -> None:
    global _pipeline_file, _env_file
    _pipeline_file = pipeline_file
    _env_file = env_file


def _lib_dir() -> str:
    with importlib.resources.as_file(
        importlib.resources.files("tkati_core") / "data"
    ) as p:
        return str(p)


@app.get("/api/manifest")
def get_manifest() -> dict:
    if not _pipeline_file:
        raise HTTPException(status_code=500, detail="No pipeline file configured")

    lib_dir = _lib_dir()
    tla_codes: dict[str, str] = {}

    if _env_file:
        with open(_env_file) as f:
            tla_codes["env"] = _jsonnet.evaluate_snippet(
                _env_file, f.read(), jpathdir=lib_dir
            )

    try:
        result = _jsonnet.evaluate_file(
            _pipeline_file, jpathdir=lib_dir, tla_codes=tla_codes
        )
        return json.loads(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
def serve_index() -> FileResponse:
    return FileResponse(_static_dir / "index.html")


if (_static_dir / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(_static_dir / "assets")), name="assets")


@app.get("/{full_path:path}")
def serve_spa(full_path: str) -> FileResponse:
    return FileResponse(_static_dir / "index.html")
