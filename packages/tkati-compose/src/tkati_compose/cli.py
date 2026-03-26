from __future__ import annotations

import importlib.resources
import json
from pathlib import Path

import _jsonnet
import click


def _lib_dir() -> str:
    with importlib.resources.as_file(
        importlib.resources.files("tkati_core") / "data"
    ) as p:
        return str(p)


def _compile(pipeline_file: str, env_file: str | None) -> dict:
    lib_dir = _lib_dir()
    tla_codes: dict[str, str] = {}
    if env_file:
        with open(env_file) as f:
            tla_codes["env"] = _jsonnet.evaluate_snippet(
                env_file, f.read(), jpathdir=lib_dir
            )
    result = _jsonnet.evaluate_file(pipeline_file, jpathdir=lib_dir, tla_codes=tla_codes)
    return json.loads(result)


@click.group("compose")
def compose_group() -> None:
    """Manage the local Docker Compose development environment."""


@compose_group.command("up")
@click.argument("file", type=click.Path(exists=True))
@click.option("--env", metavar="FILE", type=click.Path(exists=True), help="Environment TLA file")
@click.option(
    "--compose-file",
    default=".tkati/docker-compose.yml",
    metavar="FILE",
    show_default=True,
    help="Path to write the generated docker-compose.yml",
)
@click.option("--timeout", default=60, metavar="SECONDS", show_default=True, help="Redpanda startup timeout")
def up(file: str, env: str | None, compose_file: str, timeout: int) -> None:
    """Start Redpanda, wait for it to be ready, and provision topics."""
    from tkati_compose.compose import (
        generate_compose,
        provision_topics,
        start_compose,
        wait_for_ready,
        write_compose_file,
    )

    pipeline_file = str(Path(file).resolve())
    env_file = str(Path(env).resolve()) if env else None

    click.echo("Compiling manifest…")
    manifest = _compile(pipeline_file, env_file)

    compose, ports = generate_compose(manifest)
    compose_path = Path(compose_file)
    write_compose_file(compose, compose_path)
    click.echo(f"Wrote {compose_path}")

    click.echo("Starting Redpanda…")
    redpanda_services = [f"redpanda-{name}" for name in manifest["kafka_clusters"]]
    start_compose(compose_path, redpanda_services)

    for cluster_name, p in ports.items():
        click.echo(f"Waiting for {cluster_name} (admin :{p['admin_port']})…")
        try:
            wait_for_ready(p["admin_port"], timeout=timeout)
        except TimeoutError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(f"  {cluster_name} ready  (kafka localhost:{p['kafka_port']}, console http://localhost:{p['console_port']})")

    click.echo("Provisioning topics…")
    created = provision_topics(manifest, ports)
    for name in created:
        click.echo(f"  + {name}")

    click.echo("Starting nodes…")
    start_compose(compose_path)

    click.echo("Done.")


@compose_group.command("down")
@click.option(
    "--compose-file",
    default=".tkati/docker-compose.yml",
    metavar="FILE",
    show_default=True,
    help="Path to the generated docker-compose.yml",
)
@click.option("--volumes", is_flag=True, default=False, help="Also remove volumes")
def down(compose_file: str, volumes: bool) -> None:
    """Stop and remove all containers for this pipeline."""
    import subprocess
    compose_path = Path(compose_file)
    if not compose_path.exists():
        raise click.ClickException(f"{compose_path} not found — has 'tkati compose up' been run?")

    cmd = ["docker", "compose", "-f", str(compose_path), "down"]
    if volumes:
        cmd.append("--volumes")
    subprocess.run(cmd, check=True)
    click.echo("Done.")
