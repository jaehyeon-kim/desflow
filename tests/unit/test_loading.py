"""`load_context(folder, entry)`: a blueprint or a builder file, from a local folder
or a URI."""

import asyncio
import queue
import sys
import textwrap

import pytest

from desflow import SimulationContext, load_context
from desflow.blueprint.build import Preview

BLUEPRINT = """\
simulation: {sim_id: Shop, factor: 0, random_seed: 7, logical_start_time: 2026-01-01}
resources:
  packers: {current_cap: 1, max_cap: 1}
services:
  packing: {dist: exponential, rate: 2}
arrivals:
  orders: {dist: exponential, rate: 1, spawn: pack}
tasks:
  pack: {service: packing, resource: packers, payload: {packed: true}}
run: {until: 10}
"""

BUILDER = """\
from desflow import SimulationContext

from shop_settings import SIM_ID

app = SimulationContext(sim_id=SIM_ID, factor=0)

if __name__ == "__main__":
    raise SystemExit("the main guard ran")
"""


class _Records:
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _records(context):
    capture = _Records()
    context.add_egress(capture)
    context.run()
    return capture.records


def _write(folder, name, text):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


def test_a_yaml_entry_gives_the_records_of_from_yaml(tmp_path):
    path = _write(tmp_path, "main.yaml", BLUEPRINT)
    loaded = _records(load_context(tmp_path, "main.yaml"))
    assert loaded
    assert loaded == _records(SimulationContext.from_yaml(path))


def test_a_builder_file_imports_from_its_folder_without_its_main_guard(tmp_path):
    _write(tmp_path, "shop_settings.py", 'SIM_ID = "Shop"\n')
    _write(tmp_path, "main.py", BUILDER)
    first = load_context(str(tmp_path), "main.py")
    assert first.sim_id == "Shop"
    assert str(tmp_path) not in sys.path
    # Each load imports the file again, so two loads give two contexts
    assert load_context(tmp_path, "main.py") is not first


def test_desflow_app_chooses_between_contexts(tmp_path):
    text = (
        "from desflow import SimulationContext\n"
        "first = SimulationContext(sim_id='First')\n"
        "second = SimulationContext(sim_id='Second')\n"
    )
    _write(tmp_path, "main.py", text + "DESFLOW_APP = 'second'\n")
    assert load_context(tmp_path, "main.py").sim_id == "Second"
    _write(tmp_path, "several.py", text)
    with pytest.raises(
        ValueError, match=r"several SimulationContext objects \(first, second\)"
    ):
        load_context(tmp_path, "several.py")
    _write(tmp_path, "wrong.py", text + "DESFLOW_APP = 'third'\n")
    with pytest.raises(ValueError, match="DESFLOW_APP = 'third', which is not"):
        load_context(tmp_path, "wrong.py")


def test_a_file_with_no_context_is_refused(tmp_path):
    _write(
        tmp_path,
        "low_level.py",
        """\
        from desflow import DynamicRealtimeEnvironment

        if __name__ == "__main__":
            DynamicRealtimeEnvironment(factor=0).run(until=1)
        """,
    )
    with pytest.raises(ValueError, match="defines no SimulationContext"):
        load_context(tmp_path, "low_level.py")


def test_a_preview_needs_a_blueprint(tmp_path):
    _write(tmp_path, "shop_settings.py", 'SIM_ID = "Shop"\n')
    _write(tmp_path, "main.py", BUILDER)
    with pytest.raises(ValueError, match="a preview runs a YAML blueprint"):
        load_context(tmp_path, "main.py", Preview(sample=1))


def test_a_uri_folder_is_copied_and_loaded(tmp_path):
    folder = tmp_path / "sims" / "shop"
    _write(folder / "fitted", "packing.yaml", "{dist: exponential, rate: 2}\n")
    _write(
        folder,
        "main.yaml",
        BLUEPRINT.replace(
            "packing: {dist: exponential, rate: 2}",
            "packing: !include fitted/packing.yaml",
        ),
    )
    context = load_context(folder.as_uri(), "main.yaml")
    assert _records(context) == _records(load_context(folder, "main.yaml"))


def test_a_uri_without_pyarrow_names_the_extra(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with pytest.raises(ImportError, match=r'pip install "desflow\[parquet\]"'):
        load_context(tmp_path.as_uri(), "main.yaml")
