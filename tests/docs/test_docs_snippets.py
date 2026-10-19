"""Every blueprint shown in the documentation builds.

The snippets under docs/snippets/yaml/ are complete files, so each one is loaded and
run for a short time at factor 0.
"""

import sys
from pathlib import Path

import pytest

from desflow import SimulationContext

ROOT = Path(__file__).resolve().parents[2]
# Experiment files sit beside the blueprints they name; they are experiments, not models, and
# the tutorial tests run them
EXPERIMENTS = sorted((ROOT / "docs" / "snippets" / "yaml").glob("*_experiment.yaml"))
SNIPPETS = sorted(
    set((ROOT / "docs" / "snippets" / "yaml").glob("*.yaml")) - set(EXPERIMENTS)
)


def test_snippets_exist():
    assert len(SNIPPETS) >= 3


@pytest.mark.parametrize("path", SNIPPETS, ids=[p.name for p in SNIPPETS])
def test_snippet_builds_and_runs(path, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # files a sink writes stay out of the repository
    sys.modules.pop("shop_logic", None)
    app = SimulationContext.from_yaml(path)
    app.factor = 0.0
    app.run(until=app.initial_time + 200)  # 200 seconds from the clock's start


BUILD_ONLY = sorted((ROOT / "docs" / "snippets" / "yaml_build").glob("*.yaml"))


@pytest.mark.parametrize("path", BUILD_ONLY, ids=[p.name for p in BUILD_ONLY])
def test_connector_snippet_builds(path):
    """These name real services, so they are loaded and built but not run.

    Building a connector opens no connection, so no service is needed.
    """
    sys.modules.pop("routing_logic", None)
    SimulationContext.from_yaml(path)
