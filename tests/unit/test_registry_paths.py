"""The registry path forms are frozen: 1.x keeps every form a live change can set.

For each distribution family that has live parameters, a model is built from YAML with
that family in an arrival, a service and both parts of a breakdown, and the model's
registry paths are listed, with the simulation id and each name replaced by a
placeholder. Every frozen form must still be a path of one of those models. A 1.x
release may add a form, never remove or rename one.
"""

from pathlib import Path
from typing import Dict, Set

from desflow import SimulationContext

# One distribution of each family that has live parameters, as YAML. `empirical` takes
# lists, which have no path, so it is not here.
FAMILIES: Dict[str, str] = {
    "exponential": "{dist: exponential, rate: 0.5}",
    "normal": "{dist: normal, mean: 3, std: 0.5}",
    "lognormal": "{dist: lognormal, mean: 3, std: 0.5}",
    "truncated_normal": "{dist: truncated_normal, mean: 3, std: 0.5, low: 1}",
    "poisson": "{dist: poisson, mean: 3}",
    "geometric": "{dist: geometric, p: 0.5}",
    "constant": "{dist: constant, value: 4}",
    "uniform": "{dist: uniform, low: 1, high: 2}",
    "triangular": "{dist: triangular, low: 1, mode: 2, high: 3}",
    "gamma": "{dist: gamma, shape: 2, scale: 3}",
    "weibull": "{dist: weibull, shape: 2, scale: 3}",
}

MODEL = """\
simulation: {sim_id: Shop, factor: 0, random_seed: 1}
resources:
  packers:
    current_cap: 2
    max_cap: 5
    breakdown: {time_to_failure: DIST, repair: DIST}
containers:
  tank: {current_cap: 10, max_cap: 20, level: 3}
stores:
  shelf: {current_cap: 10, max_cap: 20}
services:
  pack: DIST
arrivals:
  parts: {DIST_ITEMS, spawn: part}
variables: {lot_size: 4}
tasks:
  part: {service: pack, resource: packers, payload: {n: 1}}
run: {until: 30}
"""

# The forms at 1.0: 40 of distributions (the ten live parameter names, in an arrival,
# a service, and the two parts of a breakdown), 3 of containers, 3 of resources, 2 of
# stores and 1 of variables. `<simulation>` is the `sim_id` and `<name>` the name of
# the part.
FROZEN_FORMS = {
    "<simulation>.arrival.<name>.high",
    "<simulation>.arrival.<name>.low",
    "<simulation>.arrival.<name>.mean",
    "<simulation>.arrival.<name>.mode",
    "<simulation>.arrival.<name>.p",
    "<simulation>.arrival.<name>.rate",
    "<simulation>.arrival.<name>.scale",
    "<simulation>.arrival.<name>.shape",
    "<simulation>.arrival.<name>.std",
    "<simulation>.arrival.<name>.value",
    "<simulation>.breakdown.<name>.repair.high",
    "<simulation>.breakdown.<name>.repair.low",
    "<simulation>.breakdown.<name>.repair.mean",
    "<simulation>.breakdown.<name>.repair.mode",
    "<simulation>.breakdown.<name>.repair.p",
    "<simulation>.breakdown.<name>.repair.rate",
    "<simulation>.breakdown.<name>.repair.scale",
    "<simulation>.breakdown.<name>.repair.shape",
    "<simulation>.breakdown.<name>.repair.std",
    "<simulation>.breakdown.<name>.repair.value",
    "<simulation>.breakdown.<name>.time_to_failure.high",
    "<simulation>.breakdown.<name>.time_to_failure.low",
    "<simulation>.breakdown.<name>.time_to_failure.mean",
    "<simulation>.breakdown.<name>.time_to_failure.mode",
    "<simulation>.breakdown.<name>.time_to_failure.p",
    "<simulation>.breakdown.<name>.time_to_failure.rate",
    "<simulation>.breakdown.<name>.time_to_failure.scale",
    "<simulation>.breakdown.<name>.time_to_failure.shape",
    "<simulation>.breakdown.<name>.time_to_failure.std",
    "<simulation>.breakdown.<name>.time_to_failure.value",
    "<simulation>.containers.<name>.current_cap",
    "<simulation>.containers.<name>.level",
    "<simulation>.containers.<name>.max_cap",
    "<simulation>.resources.<name>.current_cap",
    "<simulation>.resources.<name>.max_cap",
    "<simulation>.resources.<name>.queue_limit",
    "<simulation>.service.<name>.high",
    "<simulation>.service.<name>.low",
    "<simulation>.service.<name>.mean",
    "<simulation>.service.<name>.mode",
    "<simulation>.service.<name>.p",
    "<simulation>.service.<name>.rate",
    "<simulation>.service.<name>.scale",
    "<simulation>.service.<name>.shape",
    "<simulation>.service.<name>.std",
    "<simulation>.service.<name>.value",
    "<simulation>.stores.<name>.current_cap",
    "<simulation>.stores.<name>.max_cap",
    "<simulation>.variables.<name>",
}


def _forms(app: SimulationContext) -> Set[str]:
    """The paths of a model with the simulation id and each name made placeholders."""
    forms = set()
    for path in app.registry_paths():
        parts = path.split(".")
        parts[0] = "<simulation>"
        if len(parts) > 2:
            parts[2] = "<name>"
        forms.add(".".join(parts))
    return forms


def _model(dist: str) -> str:
    """The model with one distribution family in every place that takes one."""
    items = dist.strip("{}")
    return MODEL.replace("DIST_ITEMS", items).replace("DIST", dist)


def test_path_forms_are_kept(tmp_path: Path):
    path = tmp_path / "model.yaml"
    forms: Set[str] = set()
    for dist in FAMILIES.values():
        path.write_text(_model(dist))
        forms |= _forms(SimulationContext.from_yaml(path))
    missing = sorted(FROZEN_FORMS - forms)
    assert not missing, (
        "The registry path forms are frozen from 1.0: a 1.x release keeps every form "
        "and may only add one (Compatibility Policy; Live Parameters). "
        f"No longer registered: {missing}. Registered: {sorted(forms)}."
    )
