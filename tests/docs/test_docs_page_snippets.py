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


def test_storage_backends_make_their_filesystems(monkeypatch, tmp_path):
    """The GCS and Azure mappings on the Egress page build the PyArrow classes they name."""
    from pyarrow import fs

    from desflow.connectors.egress.storage import _build_filesystem

    monkeypatch.chdir(tmp_path)  # the snippet makes the folder iceberg/ here
    namespace = runpy.run_path(str(ROOT / "docs/snippets/pages/storage_backends.py"))
    classes = [
        type(_build_filesystem(egress.filesystem, fs))
        for egress in namespace["cloud"]._egress_providers
    ]

    assert classes == [fs.GcsFileSystem, fs.AzureFileSystem]


class _Idle:
    """A sink that takes its batches and keeps nothing."""

    async def run(self, egress_queue):
        import asyncio

        try:
            while True:
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


def test_backfill_live_sinks_batch_alike_in_every_form():
    """The backfill-live page gives each sink its own batching; the blueprint, the
    builder and the low-level environment end with the same settings."""
    from desflow import DynamicRealtimeEnvironment, SimulationContext

    module = runpy.run_path(str(ROOT / "docs/snippets/pages/backfill_live.py"))
    blueprint = SimulationContext.from_yaml(
        ROOT / "docs/snippets/yaml_build/backfill_live_per_sink.yaml"
    )
    for app in (module["app"], blueprint):
        assert app._egress_batch_sizes == [200_000, 500]
        assert app._egress_flush_intervals == [None, 1.0]
    env = DynamicRealtimeEnvironment(factor=0.0)
    # The sinks are stand-ins, so no broker is needed
    module["low_level_sinks"].__globals__.update(parquet=_Idle(), kafka=_Idle())
    module["low_level_sinks"](env)
    try:
        assert env.egress_batch_sizes == [200_000, 500]
        assert env.egress_flush_intervals == [1.0, 1.0]  # None takes the default
    finally:
        env.teardown()
