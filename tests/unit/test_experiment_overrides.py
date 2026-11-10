"""A misspelt override is reported at its own line, not at the line of a valid one."""

import re
import shutil
from pathlib import Path

import pytest

from desflow.analysis.runner import _checked_settings

ROOT = Path(__file__).resolve().parents[2]


def _experiment(tmp_path: Path, variant: str) -> Path:
    for name in ("experiment_shop.yaml", "observed_waits.csv"):
        source = ROOT / "examples" / ("yaml" if name.endswith(".yaml") else "")
        shutil.copy(source / name, tmp_path / name)
    text = (ROOT / "examples" / "yaml" / "experiment.yaml").read_text()
    text = text.replace("file: ../observed_waits.csv", "file: observed_waits.csv")
    text = text.replace("  three: {Shop.resources.packers.current_cap: 3}\n", variant)
    path = tmp_path / "exp.yaml"
    path.write_text(text)
    return path


@pytest.mark.parametrize(
    "overrides,bad",
    [
        (["current_cap: 3", "max_cap: 3", "max_ca: 4"], "max_ca"),
        (["max_ca: 4", "max_cap: 3", "current_cap: 3"], "max_ca"),
        (["max_cap: 3", "current_ca: 3"], "current_ca"),
    ],
)
def test_a_misspelt_override_is_reported_at_its_own_line(tmp_path, overrides, bad):
    lines = "".join(
        f"    Shop.resources.packers.{override}\n" for override in overrides
    )
    path = _experiment(tmp_path, f"  three:\n{lines}")
    text = path.read_text().splitlines()
    expected = next(i for i, line in enumerate(text, 1) if f"packers.{bad}:" in line)
    with pytest.raises(Exception) as caught:
        _checked_settings(str(path))
    message = str(caught.value)
    assert re.search(rf"exp\.yaml:{expected}\b", message), (expected, message)
