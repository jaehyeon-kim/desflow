"""The blueprint JSON Schema that ships with the package.

`blueprint.schema.json` is written by `scripts/gen_schema.py` from the models in
`models.py` and committed. This module only reads the file, so the package does not
depend on the Pydantic version that wrote it.
"""

from importlib.resources import files


def schema_text() -> str:
    """The blueprint JSON Schema, as the file in this package holds it.

    Returns:
        str: The JSON Schema (draft 2020-12) of a blueprint file, for the installed
            version of desflow.
    """
    return (
        files("desflow.blueprint")
        .joinpath("blueprint.schema.json")
        .read_text(encoding="utf-8")
    )
