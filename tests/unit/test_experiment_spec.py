"""Each cross-field check of an experiment file names its key and the key's line."""

import shutil
from pathlib import Path

import pytest

from desflow.analysis.spec import load_experiment
from desflow.blueprint.loader import BlueprintError

ROOT = Path(__file__).resolve().parents[2]
YAML = ROOT / "examples" / "yaml"


def _write(tmp_path, edit):
    for name in ("experiment_shop.yaml", "../observed_waits.csv"):
        pass
    shutil.copy(YAML / "experiment_shop.yaml", tmp_path / "experiment_shop.yaml")
    shutil.copy(
        ROOT / "examples" / "observed_waits.csv", tmp_path.parent / "observed_waits.csv"
    )
    text = (YAML / "experiment.yaml").read_text()
    text = edit(text)
    path = tmp_path / "e.yaml"
    path.write_text(text)
    return path


def _line_of(path, needle):
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        if needle in line:
            return number
    raise AssertionError(needle)


CASES = {
    "check variant": (
        lambda t: t.replace("variant: three, max: 2", "variant: thre, max: 2"),
        "checks.0.variant",
        "- {measure: tasks.pack.wait_mean, variant",
    ),
    "repeated sweep value": (
        lambda t: t.replace(
            "variants:",
            "sweep: {Shop.resources.packers.current_cap: [2, 2]}\n\nvariants:",
        ).replace(
            "  three: {Shop.resources.packers.current_cap: 3}\n", "  three: {}\n"
        ),
        "sweep",
        "sweep:",
    ),
    "warmup at until": (
        lambda t: t.replace("warmup: 3 min", "warmup: 900").replace(
            "until: 1 h", "until: 600"
        ),
        "warmup",
        "warmup:",
    ),
    "nan bound": (
        lambda t: t.replace("max: 2}", "max: .nan}"),
        "max",
        "- {measure: tasks.pack.wait_mean, variant",
    ),
}


@pytest.mark.parametrize("case", sorted(CASES))
def test_a_cross_field_check_names_its_key_and_line(tmp_path, case):
    edit, key, needle = CASES[case]
    path = _write(tmp_path, edit)
    with pytest.raises(BlueprintError) as caught:
        load_experiment(path)
    text = str(caught.value)
    assert f"e.yaml:{_line_of(path, needle)}:" in text, text
    assert key in text, text
    assert "Value error" not in text and "top level" not in text, text
