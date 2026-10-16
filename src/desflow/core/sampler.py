from typing import Optional, cast

import numpy as np

from desflow.models.params import DistributionConfig


class Sampler:
    """
    Generates random numbers based on live `DistributionConfig` objects.

    The Sampler evaluates the configuration *at the exact moment* it is called.
    This allows the simulation to dynamically respond to external parameter changes.

    Attributes:
        rng (np.random.Generator): The NumPy random number generator instance.
    """

    def __init__(self, rng: Optional[np.random.Generator] = None):
        """
        Initializes the Sampler.

        Args:
            rng (np.random.Generator, optional): A seeded NumPy random generator for reproducible runs.
                If None, nothing is drawn at random: each sample is the distribution's mean,
                or `1 / rate` for an exponential distribution.
        """
        self.rng = rng

    def sample(self, config: DistributionConfig, min_delay: float = 0.00001) -> float:
        """
        Draws a random sample using the current parameters in the provided config.

        Args:
            config (DistributionConfig): The live configuration object retrieved from the Registry.

        Returns:
            float: A sampled float representing a time duration (e.g., arrival gap or service time).
                   Returns `min_delay` if the sample is smaller than `min_delay`.
                   A std of 0 gives the mean every time.

        Raises:
            ValueError: If the `dist` type string is not supported.
        """
        # The parameters were checked by DistributionConfig and by the registry on
        # every live update, so the ones each family uses are set and in range.
        if config.dist == "exponential":
            scale = 1.0 / cast(float, config.rate)
            if self.rng is None:
                return max(min_delay, scale)
            return max(min_delay, self.rng.exponential(scale))

        if config.dist == "normal":
            m, s = cast(float, config.mean), cast(float, config.std)
            if self.rng is None:
                return max(min_delay, m)
            # With a std of 0 this returns the mean exactly
            return max(min_delay, self.rng.normal(m, s))

        if config.dist == "lognormal":
            m, s = cast(float, config.mean), cast(float, config.std)
            if self.rng is None:
                return max(min_delay, m)

            # mu/sigma conversion
            mu = np.log(m**2 / np.sqrt(s**2 + m**2))
            sigma = np.sqrt(np.log(1 + (s**2 / m**2)))
            value = self.rng.lognormal(mu, sigma)
            # With a std of 0 the draw is the mean up to rounding, so return the mean
            # exactly. The draw is still made, so later draws stay the same.
            return max(min_delay, m if s == 0 else value)

        if config.dist == "constant":
            # The same time every draw, and no random number is used
            return max(min_delay, cast(float, config.value))

        if config.dist == "truncated_normal":
            m, s = cast(float, config.mean), cast(float, config.std)
            low = cast(float, config.low)
            if self.rng is None or s == 0:
                return max(min_delay, m, low)
            return max(
                min_delay, low + s * _standard_normal_above((low - m) / s, self.rng)
            )

        raise ValueError(f"Unsupported distribution '{config.dist}'")


def _standard_normal_above(a: float, rng: np.random.Generator) -> float:
    """
    Draws a standard normal value conditioned on being at least `a`, minus `a`.

    Near or below the mean, plain draws are kept when they land above `a`. Further
    into the tail that would almost never happen, so an exponential proposal is used
    instead (Robert, 1995, "Simulation of truncated normal variables"), which keeps
    the draw exact and fast however far `a` is from the mean.
    """
    if a < 0.5:
        while True:
            z = rng.standard_normal()
            if z >= a:
                return float(z - a)
    alpha = (a + np.sqrt(a * a + 4.0)) / 2.0
    while True:
        z = a + rng.exponential(1.0 / alpha)
        if rng.random() <= np.exp(-((z - alpha) ** 2) / 2.0):
            return float(z - a)
