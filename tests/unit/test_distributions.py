"""The registry of distributions: one list of names, their moments, durations in YAML,
and one random sequence per distribution."""

import logging
import math
import re
import shlex

import numpy as np
import pytest

from desflow import SimulationContext
from desflow.blueprint.models import Distribution
from desflow.models.distributions import DISTRIBUTIONS
from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.core.sampler import Sampler, stream_key
from desflow.models.params import DistributionConfig, SimParameter, check_distribution

# One valid set of parameters per family
VALID = {
    "exponential": {"rate": 2.0},
    "normal": {"mean": 3.0, "std": 0.5},
    "lognormal": {"mean": 3.0, "std": 0.5},
    "truncated_normal": {"mean": 1.0, "std": 1.0},
    "poisson": {"mean": 3.5},
    "geometric": {"p": 0.25},
    "constant": {"value": 2.0},
    "uniform": {"low": 1.0, "high": 3.0},
    "triangular": {"low": 1.0, "mode": 2.0, "high": 4.0},
    "gamma": {"shape": 2.0, "scale": 1.5},
    "weibull": {"shape": 1.5, "scale": 2.0},
    "empirical": {"values": [1.0, 2.0, 5.0], "weights": [1.0, 2.0, 1.0]},
}


def test_yaml_the_builder_and_check_distribution_accept_the_same_names(tmp_path):
    assert set(VALID) == set(DISTRIBUTIONS)
    for name, params in VALID.items():
        check_distribution(DistributionConfig(dist=name, **params))
        SimulationContext("Shop").add_service("s", name, **params)
        Distribution(dist=name, **params)

    with pytest.raises(ValueError, match="Unsupported distribution 'beta'"):
        DistributionConfig(dist="beta", rate=1.0)
    with pytest.raises(ValueError, match="Unsupported distribution 'beta'"):
        SimulationContext("Shop").add_arrival("a", "beta", rate=1.0)
    with pytest.raises(ValueError, match="Unsupported distribution 'beta'"):
        Distribution(dist="beta", rate=1.0)
    # Keyword-only, so the order of the parameters never changes their meaning
    with pytest.raises(TypeError):
        SimulationContext("Shop").add_service("s", "uniform", 1.0, 3.0)  # type: ignore[misc]


@pytest.mark.parametrize(
    "params,mean,variance",
    [
        ({"dist": "uniform", "low": 1, "high": 3}, 2.0, 1 / 3),
        ({"dist": "triangular", "low": 1, "mode": 2, "high": 4}, 7 / 3, 7 / 18),
        ({"dist": "gamma", "shape": 2, "scale": 1.5}, 3.0, 4.5),
        # A weibull with shape 1 is an exponential
        ({"dist": "weibull", "shape": 1, "scale": 2}, 2.0, 4.0),
        ({"dist": "poisson", "mean": 3.5}, 3.5, 3.5),
        ({"dist": "geometric", "p": 0.25}, 4.0, 12.0),
        (
            {"dist": "empirical", "values": [1, 2, 5], "weights": [1, 2, 1]},
            2.5,
            2.25,
        ),
        (
            {"dist": "empirical", "edges": [0, 1, 3], "counts": [1, 3]},
            1.625,
            10 / 3 - 1.625**2,
        ),
    ],
    ids=[
        "uniform",
        "triangular",
        "gamma",
        "weibull",
        "poisson",
        "geometric",
        "values",
        "histogram",
    ],
)
def test_each_new_family_draws_with_its_declared_mean_and_variance(
    params, mean, variance
):
    config = DistributionConfig(**params)
    family = DISTRIBUTIONS[config.dist]
    assert family.mean(config) == pytest.approx(mean)
    assert family.variance(config) == pytest.approx(variance)

    sampler = Sampler(seed=7)
    draws = np.array([sampler.sample(config) for _ in range(40_000)])
    assert abs(draws.mean() - mean) < 4 * math.sqrt(variance / len(draws))
    assert draws.var() == pytest.approx(variance, rel=0.05)
    # With no generator, each sample is the mean
    assert Sampler().sample(config) == pytest.approx(mean)


@pytest.mark.parametrize(
    "params,lowest",
    [({"dist": "poisson", "mean": 0}, 0), ({"dist": "geometric", "p": 1}, 1)],
)
def test_poisson_and_geometric_draw_whole_numbers_from_their_lowest(params, lowest):
    config = DistributionConfig(**params)
    draws = [Sampler(seed=3).sample(config) for _ in range(5)]
    assert draws == [lowest] * 5 and all(type(d) is int for d in draws)
    sampler = Sampler(seed=3)
    wide = DistributionConfig(
        dist=config.dist, **{"mean": 4.0} if config.mean is not None else {"p": 0.3}
    )
    assert all(
        type(d) is int and d >= lowest
        for d in (sampler.sample(wide) for _ in range(200))
    )


@pytest.mark.parametrize(
    "params,message",
    [
        ({"dist": "poisson", "mean": -1}, "poisson mean must be 0 or more"),
        ({"dist": "geometric", "p": 0}, "geometric p must be above 0 and at most 1"),
        ({"dist": "geometric", "p": 1.5}, "geometric p must be above 0 and at most 1"),
        ({"dist": "geometric"}, "geometric needs p"),
    ],
)
def test_poisson_and_geometric_refuse_out_of_range_parameters(params, message):
    with pytest.raises(ValueError, match=message):
        DistributionConfig(**params)


@pytest.mark.parametrize("name", sorted(VALID))
def test_time_parameters_take_durations_in_yaml(name):
    params = {
        key: (
            [f"{v} min" for v in value]
            if key in ("values", "edges")
            else f"{value} min"
            if key in ("mean", "std", "low", "mode", "high", "scale", "value")
            else value
        )
        for key, value in VALID[name].items()
    }
    config = Distribution(dist=name, **params).config()
    for key, value in VALID[name].items():
        expected = (
            [v * 60 for v in value]
            if key in ("values", "edges")
            else value * 60
            if key in ("mean", "std", "low", "mode", "high", "scale", "value")
            else value
        )
        assert getattr(config, key) == expected, key


def _draws(params, sampler, path, count=5):
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(params)
    config = env.registry.get_config(path)
    return [sampler.sample(config) for _ in range(count)]


def test_adding_an_arrival_leaves_the_other_draws_unchanged():
    packing = {"packing": DistributionConfig(dist="gamma", shape=2, scale=1)}
    before = SimParameter(sim_id="Shop", service=dict(packing))
    after = SimParameter(
        sim_id="Shop",
        service={"packing": DistributionConfig(dist="gamma", shape=2, scale=1)},
        arrival={"express": DistributionConfig(dist="exponential", rate=1)},
    )

    sampler = Sampler(seed=42)
    alone = _draws(before, sampler, "Shop.service.packing")

    sampler = Sampler(seed=42)
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(after)
    express = env.registry.get_config("Shop.arrival.express")
    service = env.registry.get_config("Shop.service.packing")
    mixed = []
    for _ in range(5):
        sampler.sample(express)
        mixed.append(sampler.sample(service))
    assert mixed == alone

    # Renaming the simulation keeps the draws: the sim id is not in the sequence
    renamed = SimParameter(sim_id="Store", service=dict(packing))
    assert _draws(renamed, Sampler(seed=42), "Store.service.packing") == alone


def test_each_sequence_comes_from_the_seed_and_its_path():
    config = DistributionConfig(dist="uniform", low=0, high=1)
    config.stream = "arrival.orders"
    expected = np.random.default_rng(
        np.random.SeedSequence(42, spawn_key=stream_key("arrival.orders"))
    ).uniform(0, 1)
    assert Sampler(seed=42).sample(config) == expected
    # A seeded generator gives the same sequences, so older code keeps working
    assert Sampler(rng=np.random.default_rng(42)).sample(config) == expected
    with pytest.raises(ValueError, match="rng or seed"):
        Sampler(rng=np.random.default_rng(1), seed=1)


def test_an_unseeded_run_logs_its_seed_and_repeats_with_it(caplog):
    def run(seed):
        app = SimulationContext("Shop", factor=0, random_seed=seed)
        app.add_arrival("orders", "exponential", rate=1.0)
        draws = []

        def record(context):
            for _ in range(3):
                yield context.wait_for_arrival("orders")
                draws.append(context.env.now)

        app.add_process(record)
        app.run(until=100)
        return app, draws

    with caplog.at_level(logging.INFO, logger="desflow.builder.context"):
        app, first = run(None)
    seed = app.sampler.seed
    assert f"this run uses {seed}" in caplog.text
    assert f"Pass random_seed={seed} to repeat it" in caplog.text
    assert run(seed)[1] == first


def test_cli_seed_overrides_the_file_and_an_unseeded_run_names_the_command(
    tmp_path, caplog, monkeypatch
):
    from typer.testing import CliRunner

    from desflow.cli import app as cli

    path = tmp_path / "shop.yaml"
    path.write_text(
        "simulation: {sim_id: Shop, factor: 0}\n"
        "arrivals:\n  orders: {dist: exponential, rate: 1}\n"
        "run: {until: 5}\n",
        encoding="utf-8",
    )
    with caplog.at_level(logging.INFO, logger="desflow.builder.context"):
        result = CliRunner().invoke(cli, ["run", str(path)])
    assert result.exit_code == 0, result.output
    match = re.search(
        rf"Repeat it with: desflow run {re.escape(shlex.quote(str(path)))} --seed (\d+)",
        caplog.text,
    )
    assert match, caplog.text

    seeds = []
    monkeypatch.setattr(
        SimulationContext,
        "start",
        lambda self, until=None, drain=None: seeds.append(self.random_seed),
    )
    monkeypatch.setattr(SimulationContext, "finish", lambda self: None)
    CliRunner().invoke(cli, ["run", str(path), "--seed", match.group(1)])
    assert seeds == [int(match.group(1))]


def test_the_repeat_command_keeps_the_options_that_change_the_run(tmp_path, caplog):
    """An unseeded run given --until and --drain names them in the command that
    repeats it, since without them the repeat runs a different horizon."""
    from typer.testing import CliRunner

    from desflow.cli import app as cli

    path = tmp_path / "shop.yaml"
    path.write_text(
        "simulation: {sim_id: Shop, factor: 0}\n"
        "arrivals:\n  orders: {dist: exponential, rate: 1}\n"
        "run: {until: 5}\n",
        encoding="utf-8",
    )
    with caplog.at_level(logging.INFO, logger="desflow.builder.context"):
        result = CliRunner().invoke(
            cli, ["run", str(path), "--until", "1 min", "--drain"]
        )
    assert result.exit_code == 0, result.output
    assert re.search(
        rf"Repeat it with: desflow run {re.escape(shlex.quote(str(path)))} --seed \d+ "
        r"--until '1 min' --drain",
        caplog.text,
    ), caplog.text


@pytest.mark.parametrize(
    "parameters",
    [
        {"mean": 0, "std": 1e-160, "low": 1},
        {"mean": 0, "std": 1, "low": 1e155},
        # adjacent large integers are the same float, but the sampler subtracts them
        # as integers and so divides 1 by the std
        {"mean": 10**20, "std": 1e-200, "low": 10**20 + 1},
    ],
)
def test_a_truncated_normal_too_far_into_its_tail_is_refused_at_load(parameters):
    with pytest.raises(ValueError, match="too far above mean"):
        DistributionConfig(dist="truncated_normal", **parameters)


def test_a_truncated_normal_far_into_its_tail_that_can_be_drawn_is_accepted():
    config = DistributionConfig(dist="truncated_normal", mean=0, std=1e-150, low=1)
    assert DISTRIBUTIONS["truncated_normal"].draw(config, np.random.default_rng(1)) >= 1
