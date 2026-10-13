import numpy as np
import pytest

from dynamic_des.models.params import DistributionConfig, SimParameter


def test_sim_parameter_structure(sample_params):
    """Verify that sample_params matches the expected hierarchical structure."""
    assert isinstance(sample_params, SimParameter)
    assert sample_params.sim_id == "Shop"

    # Check Arrivals
    assert "standard" in sample_params.arrival
    assert sample_params.arrival["standard"].rate == 1 / 10.0

    # Check Services
    assert "packing" in sample_params.service
    assert sample_params.service["packing"].mean == 5.0

    # Check Resources
    assert "packers" in sample_params.resources
    assert sample_params.resources["packers"].current_cap == 2


def test_manual_numpy_sampling(sample_params):
    """Verify how a user would manually sample using NumPy and the config data."""
    rng = np.random.default_rng(42)

    # Sample from arrival (exponential)
    arrival_config = sample_params.arrival["standard"]
    # User-side logic: scale = 1/rate
    wait_time = rng.exponential(scale=1.0 / arrival_config.rate)
    assert wait_time > 0

    # Sample from service (normal)
    service_config = sample_params.service["packing"]
    # User-side logic: loc=mean, scale=std
    work_time = rng.normal(loc=service_config.mean, scale=service_config.std)
    # Manual safety clip
    assert max(0.001, work_time) > 0


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


def test_unknown_distribution_is_refused_at_build():
    # 0.16.0 accepted any name here, because a dataclass does not enforce its
    # Literal, and the sampler later returned min_delay for it.
    with pytest.raises(ValueError, match="Unsupported distribution 'deterministic'"):
        DistributionConfig(dist="deterministic", mean=0.0)


@pytest.mark.parametrize("value", [None, -1.0], ids=["missing", "negative"])
def test_constant_needs_a_value_of_zero_or_more(value):
    with pytest.raises(ValueError, match="constant value"):
        DistributionConfig(dist="constant", value=value)
