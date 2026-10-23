"""`desflow dist` and the conversion from a mean and std: each family both ways, the
refusals with their exit codes, the variability label and the clamped normal's
moments."""

import math

import numpy as np
import pytest
from typer.testing import CliRunner

from desflow.cli import app
from desflow.core.distributions import (
    DISTRIBUTIONS,
    clamped_normal_moments,
    parameters_from_moments,
    variability,
)
from desflow.models.params import DistributionConfig


@pytest.mark.parametrize(
    "family, mean, std, parameters",
    [
        ("exponential", 2.0, 2.0, {"rate": 0.5}),
        ("normal", 3.0, 0.5, {"mean": 3.0, "std": 0.5}),
        ("lognormal", 3.0, 0.5, {"mean": 3.0, "std": 0.5}),
        ("constant", 1.5, 0.0, {"value": 1.5}),
        ("constant", 0.0, 0.0, {"value": 0.0}),  # as constant --value 0
        ("uniform", 2.0, 1 / math.sqrt(3), {"low": 1.0, "high": 3.0}),
        ("gamma", 5.0, 2.5, {"shape": 4.0, "scale": 1.25}),
    ],
)
def test_each_formula_converts_both_ways(family, mean, std, parameters):
    """The parameters from a mean and std give that mean and std back through the
    family's own mean and variance."""
    found = parameters_from_moments(DISTRIBUTIONS[family], mean, std)
    assert found == pytest.approx(parameters)
    config = DistributionConfig(dist=family, **found)
    assert DISTRIBUTIONS[family].mean(config) == pytest.approx(mean)
    assert DISTRIBUTIONS[family].variance(config) == pytest.approx(std**2)


@pytest.mark.parametrize(
    "family, mean, std, message",
    [
        ("exponential", 2.0, 1.0, "std equals its mean"),
        ("constant", 2.0, 1.0, "a constant has std 0"),
        ("uniform", 1.0, 1.0, "low -0.732051, below 0"),
        ("gamma", 5.0, 0.0, "use constant for std 0"),
        ("exponential", 0.0, 0.0, "mean must be above 0 for exponential, got 0"),
        ("normal", 1.0, -1.0, "std must be 0 or more"),
        ("weibull", 1.0, 1.0, "no simple formula"),
    ],
)
def test_a_pair_the_family_cannot_have_is_refused(family, mean, std, message):
    with pytest.raises(ValueError, match=message):
        parameters_from_moments(DISTRIBUTIONS[family], mean, std)


def test_only_the_families_with_a_formula_convert():
    without = {name for name, f in DISTRIBUTIONS.items() if f.from_moments is None}
    assert without == {"truncated_normal", "triangular", "weibull", "empirical"}
    assert all(DISTRIBUTIONS[name].no_formula for name in without)


@pytest.mark.parametrize(
    "mean, std, label",
    [
        (1.0, 0.0, "none"),
        (1.0, 0.749, "low"),
        (1.0, 0.75, "moderate"),
        (1.0, 1.33, "moderate"),
        (1.0, 1.331, "high"),
        (0.0, 1.0, "undefined"),
        (0.0, 0.0, "none"),
    ],
)
def test_variability_label_boundaries(mean, std, label):
    assert variability(mean, std) == label


def test_clamped_normal_moments_match_the_sampler_draws():
    """The sampler raises every draw below min_delay to it; the formulas give the
    mean and variance of those draws."""
    draws = np.maximum(0.00001, np.random.default_rng(7).normal(1.0, 2.0, 1_000_000))
    mean, variance = clamped_normal_moments(1.0, 2.0)
    assert mean == pytest.approx(draws.mean(), rel=0.01)
    assert variance == pytest.approx(draws.var(), rel=0.01)
    assert clamped_normal_moments(5.0, 0.1) == pytest.approx((5.0, 0.01))


def _dist(*arguments):
    return CliRunner().invoke(app, ["dist", *arguments])


def test_dist_converts_in_both_directions():
    both = [
        _dist("gamma", "--mean", "5", "--std", "2.5"),
        _dist("gamma", "--shape", "4", "--scale", "1.25"),
    ]
    for result in both:
        assert result.exit_code == 0, result.output
        assert "mean 5, std 2.5" in result.output
        assert "variability: low (std / mean 0.5)" in result.output
        assert "YAML: {dist: gamma, shape: 4, scale: 1.25}" in result.output
        assert 'Python: dist="gamma", shape=4, scale=1.25' in result.output


def test_dist_lists_every_family_and_shows_one():
    listed = _dist()
    assert listed.exit_code == 0
    for name in DISTRIBUTIONS:
        assert name in listed.output
    shown = _dist("weibull")
    assert shown.exit_code == 0
    assert "rules: shape > 0, scale > 0" in shown.output
    assert "from a mean and std: no simple formula" in shown.output


def test_dist_shows_the_clamped_normal():
    result = _dist("normal", "--mean", "1", "--std", "2")
    assert result.exit_code == 0
    assert "which gives mean 1.3956, std 1.48787" in result.output


def test_dist_names_a_draw_raised_to_the_minimum_delay():
    result = _dist("constant", "--value", "0")
    assert result.exit_code == 0
    assert "every draw is 0, raised to the minimum delay 0.00001 s" in result.output
    unclamped = _dist("constant", "--value", "2")
    assert "every draw is 2\n" in unclamped.output


@pytest.mark.parametrize(
    "arguments, code, message",
    [
        (["gama"], 2, "did you mean 'gamma'"),
        (["constant", "--value", "-1"], 2, "must be 0 or more"),
        (["gamma", "--rate", "1"], 2, "gamma does not take rate"),
        (["gamma", "--mean", "5", "--std", "2", "--shape", "1"], 2, "together"),
        (["exponential", "--mean", "2", "--std", "1"], 2, "std equals its mean"),
        (["weibull", "--mean", "3"], 1, "no simple formula"),
    ],
)
def test_dist_refusals_and_their_exit_codes(arguments, code, message):
    result = _dist(*arguments)
    assert result.exit_code == code
    assert message in " ".join(result.output.split())
