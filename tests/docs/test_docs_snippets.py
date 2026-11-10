"""Every blueprint shown in the documentation builds.

The snippets under docs/snippets/yaml/ are complete files, so each one is loaded and
run for a short time at factor 0, except those another docs test already runs.
"""

import sys
from pathlib import Path

import pytest

from desflow import SimulationContext

ROOT = Path(__file__).resolve().parents[2]
YAML = ROOT / "docs" / "snippets" / "yaml"
# Experiment files sit beside the blueprints they name; they are experiments, not
# models, and the tutorial tests run them
EXPERIMENTS = sorted(YAML.glob("*_experiment.yaml"))
# A blueprint that shows a load error fails on purpose; test_docs_record_blocks.py
# checks the error its page shows
ERRORS = sorted(YAML.glob("*_error.yaml"))
# test_docs_tutorials.py and test_docs_guides.py run these in full and check the
# answer their page gives, so a short run here would check nothing more
RUN_BY_OTHER_TESTS = {
    "avro_and_pydantic.yaml",
    "changing_demand.yaml",
    "check_against_theory.yaml",
    "checking_a_model.yaml",
    "choosing_inputs.yaml",
    "cloud_storage.yaml",
    "call_python.yaml",
    "cloud_storage_daily.yaml",
    "data_at_scale.yaml",
    "competing_resources.yaml",
    "complete_model.yaml",
    "complex_routing.yaml",
    "complex_routing_router.yaml",
    "end_conditions.yaml",
    "filtered_inventory.yaml",
    "first_shop.yaml",
    "fit_a_distribution.yaml",
    "fit_the_inputs.yaml",
    "flow_of_tasks.yaml",
    "iceberg_tables.yaml",
    "interruptions.yaml",
    "json_schema_and_protobuf.yaml",
    "json_schema_and_protobuf_glue.yaml",
    "kafka_security.yaml",
    "kafka_security_msk.yaml",
    "kafka_security_mtls.yaml",
    "kafka_security_registry.yaml",
    "live_changes.yaml",
    "live_changes_live.yaml",
    "local_replay.yaml",
    "measuring_a_run.yaml",
    "patience.yaml",
    "performance_tuning.yaml",
    "priorities.yaml",
    "process_interaction.yaml",
    "queues_and_capacity.yaml",
    "randomness_and_seeds.yaml",
    "reneging_tie.yaml",
    "run_before.yaml",
    "stock.yaml",
    "streaming_out.yaml",
    "test_a_model.yaml",
    "test_a_pipeline.yaml",
    "test_a_model_tests.yaml",
    "trusting_results.yaml",
    "wait_all.yaml",
    "wait_first.yaml",
    "wait_for_one_or_all_example.yaml",
    "wait_tie.yaml",
    "write_a_connector.yaml",
}
SNIPPETS = sorted(
    p
    for p in set(YAML.glob("*.yaml")) - set(EXPERIMENTS) - set(ERRORS)
    if p.name not in RUN_BY_OTHER_TESTS
)


def test_snippets_exist():
    assert len(SNIPPETS) >= 3


def test_blueprints_left_out_are_named_by_the_tests_that_run_them():
    """A blueprint drops out of the short run only while another test still names it."""
    here = Path(__file__).parent
    others = "".join(
        path.read_text(encoding="utf-8")
        for path in [
            here / "test_docs_tutorials.py",
            *here.glob("test_docs_guides*.py"),
        ]
    )
    missing = sorted(
        name
        for name in RUN_BY_OTHER_TESTS
        if not (YAML / name).exists() or f'/ "{name}"' not in others
    )
    assert not missing, f"run these in test_docs_snippets.py again: {missing}"


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
