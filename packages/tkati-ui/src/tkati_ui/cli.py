from __future__ import annotations

import threading
import webbrowser
from pathlib import Path

import click


@click.command("ui")
@click.argument("file", type=click.Path(exists=True))
@click.option("--env", metavar="FILE", type=click.Path(exists=True), help="Environment TLA file")
@click.option("--port", default=7749, metavar="PORT", show_default=True, help="Port to listen on")
@click.option("--no-open", is_flag=True, help="Do not open browser automatically")
def ui(file: str, env: str | None, port: int, no_open: bool) -> None:
    """Open the pipeline graph UI in a browser."""
    pipeline_file = str(Path(file).resolve())
    env_file = str(Path(env).resolve()) if env else None

    from tkati_ui import server
    server.configure(pipeline_file=pipeline_file, env_file=env_file)

    url = f"http://localhost:{port}"
    click.echo(f"tkati ui  →  {url}")

    if not no_open:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    import uvicorn
    uvicorn.run(server.app, host="127.0.0.1", port=port, log_level="warning")


def main() -> None:
    ui()
