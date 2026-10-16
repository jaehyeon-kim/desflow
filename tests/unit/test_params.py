import pytest

from desflow.models.params import DistributionConfig


def test_distribution_parameters_are_floats():
    """A whole number is stored as a float, so a later live update keeps its fraction."""
    config = DistributionConfig(dist="exponential", rate=2)
    assert isinstance(config.rate, float)

    config = DistributionConfig(dist="lognormal", mean=3, std=1)
    assert isinstance(config.mean, float)
    assert isinstance(config.std, float)


@pytest.mark.parametrize(
    "kwargs,fragment",
    [
        ({"dist": "exponential", "rate": 0}, "rate"),
        ({"dist": "exponential", "rate": -2}, "rate"),
        ({"dist": "exponential"}, "rate"),
        ({"dist": "normal", "mean": 1.0, "std": -1.0}, "std"),
        ({"dist": "normal", "std": 1.0}, "mean"),
        ({"dist": "lognormal", "mean": 0.0, "std": 1.0}, "mean"),
        ({"dist": "lognormal", "mean": -1.0, "std": 1.0}, "mean"),
        ({"dist": "lognormal", "mean": 3.0, "std": -0.5}, "std"),
        ({"dist": "uniform", "low": 3, "high": 3}, "low < high"),
        ({"dist": "uniform", "low": 1}, "needs high"),
        ({"dist": "triangular", "low": 1, "mode": 5, "high": 4}, "low <= mode"),
        ({"dist": "triangular", "low": 1, "high": 4}, "needs mode"),
        ({"dist": "gamma", "shape": 0, "scale": 1}, "shape"),
        ({"dist": "weibull", "shape": 1, "scale": -1}, "scale"),
        ({"dist": "empirical", "values": []}, "at least one value"),
        ({"dist": "empirical", "values": [1, 2], "weights": [1]}, "one weight"),
        ({"dist": "empirical", "values": [1, 2], "weights": [1, -1]}, "0 or more"),
        ({"dist": "empirical", "values": [1], "weights": [0]}, "more than 0"),
        ({"dist": "empirical", "edges": [0, 2, 1], "counts": [1, 1]}, "increase"),
        ({"dist": "empirical", "edges": [0, 1, 2], "counts": [1]}, "one count"),
        ({"dist": "empirical", "values": [1], "edges": [0, 1]}, "not both"),
        # A parameter the family does not take is refused, not ignored
        ({"dist": "normal", "mean": 1.0, "rate": 2.0}, "does not take rate"),
    ],
)
def test_distribution_config_refuses_invalid_parameters(kwargs, fragment):
    """An invalid parameter fails when the config is built, not later in the sampler."""
    with pytest.raises(ValueError, match=fragment):
        DistributionConfig(**kwargs)


def test_distribution_config_accepts_a_zero_std():
    """A std of 0 is valid: the distribution is a constant equal to its mean."""
    assert DistributionConfig(dist="normal", mean=2.0, std=0).std == 0.0
    assert DistributionConfig(dist="lognormal", mean=2.0, std=0).std == 0.0
    # A missing std is a constant too, as it was before validation was added.
    assert DistributionConfig(dist="normal", mean=2.0).std == 0.0


@pytest.mark.parametrize("value", [None, -1.0], ids=["missing", "negative"])
def test_constant_needs_a_value_of_zero_or_more(value):
    with pytest.raises(ValueError, match="constant value"):
        DistributionConfig(dist="constant", value=value)
