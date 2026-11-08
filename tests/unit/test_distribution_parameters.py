"""Distribution parameters take finite numbers, not bools, and not values whose draw,
mean or variance overflows a float. A time a family draws between is not below 0."""

import pytest

from desflow.core.context import SimulationContext
from desflow.models.params import DistributionConfig

nan, inf = float("nan"), float("inf")

HEAD = (
    "simulation: {sim_id: B, factor: 0, random_seed: 1}\n"
    "arrivals: {orders: {dist: exponential, rate: 1, spawn: serve}}\n"
    "tasks: {serve: {service: work, payload: {}}}\nrun: {until: 5}\n"
)

SERVICES = [
    "{dist: exponential, rate: true}",
    "{dist: normal, mean: .nan, std: 1}",
    "{dist: uniform, low: 0, high: .inf}",
    "{dist: uniform, low: -1, high: 3}",
    "{dist: triangular, low: -1, mode: 0, high: 3}",
    "{dist: truncated_normal, mean: 1, std: 1, low: -1}",
    "{dist: empirical, values: [-1, 2]}",
    "{dist: weibull, shape: 0.005, scale: 1}",
    "{dist: poisson, mean: 1.0e+20}",
    "{dist: lognormal, mean: 1.0e+200, std: 1}",
]


@pytest.mark.parametrize("service", SERVICES)
def test_yaml_refuses_the_parameter_with_the_line(tmp_path, service):
    path = tmp_path / "m.yaml"
    path.write_text(HEAD + f"services: {{work: {service}}}\n")
    with pytest.raises(Exception, match=r"m\.yaml:\d+: services\.work"):
        SimulationContext.from_yaml(path)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"dist": "exponential", "rate": True},
        {"dist": "normal", "mean": nan},
        {"dist": "uniform", "low": 0, "high": inf},
        {"dist": "uniform", "low": -1, "high": 3},
        {"dist": "empirical", "values": [nan]},
        {"dist": "weibull", "shape": 0.005, "scale": 1},
        {"dist": "poisson", "mean": 1e20},
        {"dist": "lognormal", "mean": 1e200, "std": 1},
    ],
)
def test_the_config_refuses_the_parameter_by_name(kwargs):
    with pytest.raises(ValueError, match=kwargs["dist"]):
        DistributionConfig(**kwargs)


def test_ordinary_parameters_still_load():
    DistributionConfig(dist="uniform", low=0, high=2)
    DistributionConfig(dist="weibull", shape=1.5, scale=2)
    DistributionConfig(dist="lognormal", mean=2, std=1)


BAD = [True, float("nan"), float("inf")]


@pytest.mark.parametrize("bad", BAD, ids=repr)
def test_the_builder_refuses_an_arrival_parameter_by_name(bad):
    from desflow.core.context import SimulationContext

    with pytest.raises(ValueError, match="rate"):
        SimulationContext("S", factor=0).add_arrival(
            "a", "exponential", rate=bad, spawn="t"
        )


@pytest.mark.parametrize("written", ["true", ".nan", ".inf"])
def test_a_scenario_step_refuses_a_parameter_at_its_line(written, tmp_path):
    from desflow.core.context import SimulationContext

    path = tmp_path / "m.yaml"
    path.write_text(
        "simulation: {sim_id: B, factor: 0, random_seed: 1}\n"
        "services: {work: {dist: exponential, rate: 1}}\n"
        "arrivals: {orders: {dist: exponential, rate: 1, spawn: serve}}\n"
        "tasks: {serve: {service: work, payload: {}}}\n"
        f"scenario: [{{at: 2, path: B.service.work.rate, value: {written}}}]\n"
        "run: {until: 5}\n"
    )
    with pytest.raises(Exception, match=r"m\.yaml:5: .*rate"):
        SimulationContext.from_yaml(path)
