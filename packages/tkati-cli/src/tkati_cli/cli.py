import importlib.resources
import sys
from pathlib import Path

import _jsonnet
import click


def _lib_dir() -> str:
    with importlib.resources.as_file(
        importlib.resources.files("tkati_core") / "data"
    ) as p:
        return str(p)


def _eval(jsonnet_file: str, tla_code_files: dict[str, str]) -> str:
    lib_dir = _lib_dir()
    tla_codes = {}
    for key, path in tla_code_files.items():
        with open(path) as f:
            tla_codes[key] = _jsonnet.evaluate_snippet(
                path, f.read(), jpathdir=lib_dir
            )
    return _jsonnet.evaluate_file(
        jsonnet_file,
        jpathdir=lib_dir,
        tla_codes=tla_codes,
    )


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--env", metavar="FILE", type=click.Path(exists=True), help="Environment TLA file")
@click.option("--out", metavar="FILE", type=click.Path(), help="Write output to file instead of stdout")
def compile(file: str, env: str | None, out: str | None) -> None:
    """Compile a pipeline to a manifest."""
    result = _eval(file, {"env": env} if env else {})
    if out:
        Path(out).write_text(result)
    else:
        click.echo(result, nl=False)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--env", metavar="FILE", type=click.Path(exists=True), help="Environment TLA file")
@click.option("--out", metavar="FILE", type=click.Path(), help="Write output to file instead of stdout")
def terraform(file: str, env: str | None, out: str | None) -> None:
    """Generate Terraform JSON from a pipeline."""
    abs_file = str(Path(file).resolve())
    snippet = f"""\
function(env)
  local toTerraform = import 'tkati-terraform.libsonnet';
  local pipeline    = import @'{abs_file}';
  toTerraform(pipeline(env))
"""
    lib_dir = _lib_dir()
    tla_codes: dict[str, str] = {}
    if env:
        with open(env) as f:
            tla_codes["env"] = _jsonnet.evaluate_snippet(
                env, f.read(), jpathdir=lib_dir
            )
    result = _jsonnet.evaluate_snippet(
        "<tkati-terraform>", snippet, jpathdir=lib_dir, tla_codes=tla_codes
    )
    if out:
        Path(out).write_text(result)
    else:
        click.echo(result, nl=False)




def main() -> None:
    cli()


if __name__ == "__main__":
    main()
