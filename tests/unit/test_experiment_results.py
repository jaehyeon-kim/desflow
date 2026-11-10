"""An experiment reports each published statistic in the unit and over the population
it has.

A resource's `downtime` is the seconds it was down, summed over the measured horizon
(`core/statistics.py` publishes `obj.downtime`, the sum of its down periods), so
`results.csv`, `summary.csv` and an observed value use the unit `s`, not `count`.
"""

import csv
from pathlib import Path

import pytest

from desflow import run_experiment

ROOT = Path(__file__).resolve().parents[2]

EXPERIMENT = """\
experiment: down
question: How long is the station down?
model: model.yaml
assumptions: ["Failures are random."]
inputs: {Shop.service.packing: "40 packing times measured at the bench"}
until: 300
replications: 2
seed: 3
measures: [resources.station.downtime, resources.station.failures]
variants: {base: {}}
observed: {file: observed.csv}
limits: ["Five minutes of one station."]
output: out
"""


@pytest.fixture
def folder(tmp_path: Path) -> Path:
    text = (ROOT / "examples" / "yaml" / "breakdowns.yaml").read_text(encoding="utf-8")
    (tmp_path / "model.yaml").write_text(
        text.replace("egress:\n  - {type: Console}\n", ""), encoding="utf-8"
    )
    (tmp_path / "experiment.yaml").write_text(EXPERIMENT, encoding="utf-8")
    return tmp_path


def _rows(path: Path) -> dict:
    with path.open(newline="", encoding="utf-8") as handle:
        return {row["statistic"]: row for row in csv.DictReader(handle)}


def test_downtime_is_in_seconds_in_every_file_and_in_an_observed_value(folder):
    (folder / "observed.csv").write_text(
        "measure,value,unit,population,horizon\n"
        "resources.station.downtime,30,s,sum over the horizon,300\n",
        encoding="utf-8",
    )
    run_experiment(folder / "experiment.yaml")
    for name in ("results.csv", "summary.csv"):
        rows = _rows(folder / "out" / name)
        assert rows["resources.station.downtime"]["unit"] == "s", name
        assert rows["resources.station.failures"]["unit"] == "count", name
    results = _rows(folder / "out" / "results.csv")
    assert results["resources.station.downtime"]["population"] == "sum over the horizon"


def _replication_values(path: Path, statistic: str) -> list:
    with path.open(newline="", encoding="utf-8") as handle:
        return [
            float(row["value"])
            for row in csv.DictReader(handle)
            if row["statistic"] == statistic
        ]


def test_a_warmup_resets_the_downtime_and_the_failures(folder):
    # The warm-up resets a resource's downtime and failures, as it does the
    # averages, so neither counts the warm-up
    text = (folder / "experiment.yaml").read_text(encoding="utf-8")
    text = text.replace("observed: {file: observed.csv}\n", "")
    (folder / "experiment.yaml").write_text(text, encoding="utf-8")
    run_experiment(folder / "experiment.yaml")
    whole = {
        name: _replication_values(folder / "out" / "results.csv", name)
        for name in ("resources.station.downtime", "resources.station.failures")
    }
    (folder / "experiment.yaml").write_text(
        text.replace("until: 300\n", "until: 300\nwarmup: 150\n").replace(
            "output: out", "output: warm"
        ),
        encoding="utf-8",
    )
    run_experiment(folder / "experiment.yaml")
    downtime = _replication_values(
        folder / "warm" / "results.csv", "resources.station.downtime"
    )
    failures = _replication_values(
        folder / "warm" / "results.csv", "resources.station.failures"
    )
    assert downtime and failures
    # Each replication measures only the 150 s after the warm-up
    assert all(value <= 150 for value in downtime)
    assert all(
        warm <= full
        for warm, full in zip(downtime, whole["resources.station.downtime"])
    )
    assert sum(failures) < sum(whole["resources.station.failures"])
