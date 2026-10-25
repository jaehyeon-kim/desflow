"""`desflow schema`: prints the blueprint JSON Schema."""

import typer


# The examples --help prints after the options, as odctl's help does
_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# Save the blueprint schema for an editor[/dim]\n
  $ [bold cyan]desflow schema > blueprint.schema.json[/bold cyan]
"""


def register(app: typer.Typer) -> None:
    """Adds the `schema` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("schema", rich_help_panel="Files", epilog=_EPILOG)(_schema)


def _schema() -> None:
    """Print the blueprint JSON Schema."""
    from desflow.blueprint.schema import schema_text

    typer.echo(schema_text(), nl=False)
