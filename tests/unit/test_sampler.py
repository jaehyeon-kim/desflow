import math
import numpy as np
import pytest

from desflow.core.sampler import Sampler
from desflow.models.params import DistributionConfig


@pytest.fixture
def rng():
    """Provides a seeded NumPy generator for reproducible stochastic tests."""
    return np.random.default_rng(42)


@pytest.fixture
def sampler(rng):
    """Provides a Sampler initialized with an RNG."""
    return Sampler(rng=rng)


@pytest.fixture
def deterministic_sampler():
    """Provides a Sampler with no RNG for deterministic tests."""
    return Sampler(rng=None)


def test_sampler_deterministic_fallback(deterministic_sampler):
    """Verify that without an RNG, the sampler returns the mean (clipped)."""
    # Exponential: mean = 1/0.1 = 10.0
    exp_cfg = DistributionConfig(dist="exponential", rate=0.1)
    assert deterministic_sampler.sample(exp_cfg) == 10.0

    # Normal: mean = 5.0
    norm_cfg = DistributionConfig(dist="normal", mean=5.0, std=1.0)
    assert deterministic_sampler.sample(norm_cfg) == 5.0

    # Lognormal: mean = 3.0
    log_cfg = DistributionConfig(dist="lognormal", mean=3.0, std=0.5)
    assert deterministic_sampler.sample(log_cfg) == 3.0


def test_sampler_min_delay_clipping(sampler):
    """Verify that min_delay correctly clips small or negative values."""
    # Create a normal distribution that would produce a negative value
    # (Seed 42 first normal sample is ~0.3, so mean -5.0 will be negative)
    cfg = DistributionConfig(dist="normal", mean=-5.0, std=1.0)

    # Test with default 0.1
    assert sampler.sample(cfg, min_delay=0.1) == 0.1

    # Test with custom 1.0
    assert sampler.sample(cfg, min_delay=1.0) == 1.0


def test_sampler_stochastic_exponential(sampler):
    """Verify exponential sampling (rate to scale conversion)."""
    rate = 0.5  # Mean should be 2.0
    cfg = DistributionConfig(dist="exponential", rate=rate)

    samples = [sampler.sample(cfg) for _ in range(1000)]
    assert 1.9 < np.mean(samples) < 2.1
    assert min(samples) >= 0.00001


def test_sampler_stochastic_lognormal(sampler):
    """Verify lognormal sampling returns values near the desired mean."""
    desired_mean = 10.0
    desired_std = 2.0
    cfg = DistributionConfig(dist="lognormal", mean=desired_mean, std=desired_std)

    samples = [sampler.sample(cfg) for _ in range(1000)]
    # Log-normal mean is usually close to desired mean in large samples
    assert 9.5 < np.mean(samples) < 10.5
    assert min(samples) > 0  # Log-normal is strictly positive


def test_lognormal_with_zero_std_is_a_constant(sampler):
    """A std of 0 gives the mean every time; it used to be replaced by 0.1."""
    cfg = DistributionConfig(dist="lognormal", mean=3.0, std=0.0)
    assert {sampler.sample(cfg) for _ in range(100)} == {3.0}


def test_normal_with_zero_std_is_a_constant(sampler):
    """A std of 0 gives the mean every time."""
    cfg = DistributionConfig(dist="normal", mean=3.0, std=0.0)
    assert {sampler.sample(cfg) for _ in range(100)} == {3.0}


def test_sampler_unknown_distribution_raises(sampler):
    """An unknown family raises, as the docstring says, instead of returning min_delay."""
    cfg = DistributionConfig(dist="normal", mean=1.0, std=0.0)
    cfg.dist = "deterministic"
    with pytest.raises(ValueError, match="deterministic"):
        sampler.sample(cfg)


def _truncated_mean(mean, std, low):
    a = (low - mean) / std
    density = math.exp(-a * a / 2) / math.sqrt(2 * math.pi)
    below = 0.5 * (1 + math.erf(a / math.sqrt(2)))
    return mean + std * density / (1 - below)


@pytest.mark.parametrize(
    "mean, std, low",
    [(1.0, 1.0, 0.0), (5.0, 2.0, 1.0), (0.0, 1.0, 3.0)],
    ids=["near the mean", "below the mean", "far in the tail"],
)
def test_truncated_normal_draws_again_instead_of_clamping(mean, std, low):
    # Defect 4: a plain normal clamps to min_delay, which piles draws at the limit
    # and raises the mean. A truncated normal draws again, so no draw sits at `low`
    # and the mean is the truncated distribution's own mean.
    sampler = Sampler(rng=np.random.default_rng(0))
    config = DistributionConfig(dist="truncated_normal", mean=mean, std=std, low=low)
    draws = np.array([sampler.sample(config) for _ in range(50_000)])
    assert draws.min() >= low
    assert (draws == low).mean() < 0.001
    standard_error = draws.std() / math.sqrt(len(draws))
    assert abs(draws.mean() - _truncated_mean(mean, std, low)) < 4 * standard_error


def test_truncated_normal_with_zero_std_is_a_constant():
    sampler = Sampler(rng=np.random.default_rng(0))
    config = DistributionConfig(dist="truncated_normal", mean=4.0, std=0.0, low=1.0)
    assert {sampler.sample(config) for _ in range(10)} == {4.0}


def test_truncated_normal_low_defaults_to_zero():
    assert DistributionConfig(dist="truncated_normal", mean=1.0, std=1.0).low == 0.0


def test_constant_returns_its_value_and_draws_nothing():
    rng = np.random.default_rng(0)
    sampler = Sampler(rng=rng)
    state = rng.bit_generator.state
    config = DistributionConfig(dist="constant", value=2.5)
    assert {sampler.sample(config) for _ in range(10)} == {2.5}
    assert rng.bit_generator.state == state


def test_constant_of_zero_is_the_smallest_delay():
    sampler = Sampler(rng=np.random.default_rng(0))
    assert sampler.sample(DistributionConfig(dist="constant", value=0.0)) == 0.00001
