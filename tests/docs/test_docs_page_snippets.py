"""Every snippet file under docs/snippets/pages/ runs as a script.

The pages show copies of their sections, which test_docs_match_examples.py keeps equal
to the files. This test runs the files themselves, so the code on a page works.
Building a connector opens no connection, so no service is needed. The one exception
is the Iceberg REST catalog, which asks its server for settings when it is created,
so a stand-in replaces it.
"""

import runpy
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
PAGES = sorted((ROOT / "docs" / "snippets" / "pages").glob("*.py"))


def test_there_are_page_snippets():
    assert PAGES


@pytest.mark.parametrize("script", PAGES, ids=[p.name for p in PAGES])
def test_page_snippet_runs(script, monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    monkeypatch.chdir(tmp_path)  # files a snippet writes stay out of the repository
    with mock.patch("pyiceberg.catalog.rest.RestCatalog"):
        runpy.run_path(str(script), run_name="__main__")


def test_failures_print_what_the_page_says(capsys):
    """The Failures page quotes what its two runs print."""
    runpy.run_path(str(ROOT / "docs/snippets/pages/failures.py"), run_name="__main__")
    printed = capsys.readouterr().out.splitlines()
    assert printed == [
        "pack failed: after the cut-off",
        "closing failed: the till did not balance",
    ]
