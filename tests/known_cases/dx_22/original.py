"""DX-22: A catalog that a variable chooses.

The original is the snippet's own mapping: `CATALOGS` of
docs/snippets/guides/iceberg_tables_catalog.py, which the variable `ICEBERG_CATALOG` picks
from. `check` loads the model with the variable unset and set, and compares the type of
the catalog and the scheme of its address with the snippet's.
"""

import os
import sys
import tempfile
from pathlib import Path

from desflow import SimulationContext

HERE = Path(__file__).parent
SNIPPETS = HERE.parents[2] / "docs" / "snippets" / "guides"


def _snippet_catalogs():
    """The snippet's mappings, imported from a scratch folder because it makes `iceberg/`."""
    before = os.getcwd()
    sys.path.insert(0, str(SNIPPETS))
    with tempfile.TemporaryDirectory() as scratch:
        os.chdir(scratch)
        try:
            sys.modules.pop("iceberg_tables_catalog", None)
            from iceberg_tables_catalog import CATALOGS

            return {name: dict(entry) for name, entry in CATALOGS.items()}
        finally:
            os.chdir(before)
            sys.path.remove(str(SNIPPETS))


def check(records):
    """With the variable unset the model has the `sqlite` catalog, and with it set to `rest`
    the `rest` catalog, each of the type and the address scheme the snippet gives."""
    snippet = _snippet_catalogs()
    saved = os.environ.pop("ICEBERG_CATALOG", None)
    try:
        for variable, name in ((None, "sqlite"), ("rest", "rest")):
            if variable:
                os.environ["ICEBERG_CATALOG"] = variable
            else:
                os.environ.pop("ICEBERG_CATALOG", None)
            app = SimulationContext.from_yaml(HERE / "model.yaml")
            made = app._sinks.egress[0].provider.catalog_properties
            assert made["name"] == name
            assert made["type"] == snippet[name]["type"]
            assert made["uri"].split(":")[0] == snippet[name]["uri"].split(":")[0]
    finally:
        os.environ.pop("ICEBERG_CATALOG", None)
        if saved is not None:
            os.environ["ICEBERG_CATALOG"] = saved
    assert any(r["stream_type"] == "event" for r in records)
