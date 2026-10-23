"""`desflow schema`: prints the blueprint JSON Schema."""

import typer


def register(app: typer.Typer) -> None:
    """Adds the `schema` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("schema")(_schema)


def _schema() -> None:
    """Print the blueprint JSON Schema."""
    from desflow.blueprint.schema import schema_text

    typer.echo(schema_text(), nl=False)
