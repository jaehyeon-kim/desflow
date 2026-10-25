"""`desflow examples`: lists the examples, or downloads one."""

import shlex
from pathlib import Path
from typing import Annotated, Optional

import typer

_examples_app = typer.Typer(
    help="List the examples, or download one at the version you have installed.",
    invoke_without_command=True,
)


# The examples --help prints after the options, as odctl's help does
_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# List the examples[/dim]\n
  $ [bold cyan]desflow examples list[/bold cyan]\n\n
  [dim]# Download the YAML version of one[/dim]\n
  $ [bold cyan]desflow examples get local --api yaml[/bold cyan]
"""


_LIST_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# List the examples, with what each shows and the service it needs[/dim]\n
  $ [bold cyan]desflow examples list[/bold cyan]
"""

_GET_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# Download the three versions of an example into ./local/[/dim]\n
  $ [bold cyan]desflow examples get local[/bold cyan]\n\n
  [dim]# Download only the YAML version into another folder[/dim]\n
  $ [bold cyan]desflow examples get local --api yaml --dest work[/bold cyan]\n\n
  [dim]# Download it from the main branch[/dim]\n
  $ [bold cyan]desflow examples get local --ref main[/bold cyan]
"""


def register(app: typer.Typer) -> None:
    """Adds the `examples` command and its `list` and `get` sub-commands to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.add_typer(
        _examples_app, name="examples", rich_help_panel="Files", epilog=_EPILOG
    )


@_examples_app.callback()
def _examples(ctx: typer.Context) -> None:
    """List the examples, as `desflow examples list` does, when no command follows."""
    if ctx.invoked_subcommand is None:
        _examples_list()


@_examples_app.command("list", epilog=_LIST_EPILOG)
def _examples_list() -> None:
    """List the examples, with what each shows and what it needs."""
    from desflow.blueprint.examples import EXAMPLES

    width = max(len(name) for name in EXAMPLES)
    for example in EXAMPLES.values():
        needs = f"odctl up {example.profile}" if example.profile else "no service"
        typer.echo(f"{example.name:<{width}}  {example.shows} ({needs})")
    typer.echo("\nDownload one with: desflow examples get <name>")


@_examples_app.command("get", epilog=_GET_EPILOG)
def _examples_get(
    name: Annotated[
        str, typer.Argument(help="The example, as `desflow examples` lists it.")
    ],
    api: Annotated[
        Optional[str],
        typer.Option(help="Only this version: yaml, builder or low-level."),
    ] = None,
    dest: Annotated[Path, typer.Option(help="The folder to download into.")] = Path(
        "."
    ),
    ref: Annotated[
        Optional[str],
        typer.Option(
            help="A git tag or branch. Defaults to the installed release's tag."
        ),
    ] = None,
    force: Annotated[
        bool, typer.Option(help="Replace files that are already there.")
    ] = False,
) -> None:
    """Download an example into <dest>/<name>/<api>/, at the installed release."""
    from desflow import __version__
    from desflow.blueprint.examples import (
        APIS,
        EXAMPLES,
        download,
        release_ref,
        run_hint,
    )

    example = EXAMPLES.get(name)
    if example is None:
        raise typer.BadParameter(
            f"no example called '{name}'; choose one of {', '.join(EXAMPLES)}"
        )
    if api is not None and api not in APIS:
        raise typer.BadParameter(f"--api must be one of {', '.join(APIS)}")
    apis = [api] if api else list(APIS)
    ref = ref or release_ref(__version__)
    try:
        written = download(example, apis, dest, ref, force=force)
    except (FileExistsError, RuntimeError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from None
    for path in written:
        typer.echo(f"wrote {path}")
    typer.echo("")
    if example.extras:
        typer.echo(f'Install: pip install "desflow[{example.extras}]"')
    if example.explain and "yaml" in apis:
        folder = shlex.quote((dest / example.name / "yaml").as_posix())
        typer.echo(
            f"Find the bottleneck, with no run: cd {folder} && "
            f"desflow explain {example.explain}"
        )
    if example.profile:
        typer.echo(f"Start its service: odctl up {example.profile}")
    for each in apis:
        typer.echo(f"Run the {each} version: {run_hint(example, each, dest)}")
    for note in example.notes:
        typer.echo(note)
