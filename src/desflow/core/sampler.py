import zlib
from typing import Dict, List, Optional

import numpy as np

from desflow.config import MIN_DELAY
from desflow.core.distributions import get_distribution
from desflow.models.params import DistributionConfig


class Sampler:
    """
    Generates random numbers based on live `DistributionConfig` objects.

    The Sampler evaluates the configuration *at the exact moment* it is called.
    This allows the simulation to dynamically respond to external parameter changes.

    Each distribution draws from a random sequence of its own, made from the run's
    seed and the distribution's registry path without the sim id (`arrival.orders`,
    `service.packing`): `SeedSequence(seed, spawn_key=(CRC32 of the path,))`. Adding
    an arrival therefore leaves every other distribution's draws unchanged, so two
    versions of a model run with one seed draw the same numbers for every
    distribution they share (common random numbers).

    Attributes:
        rng (Optional[np.random.Generator]): The run's main generator, for draws in
            your own code. Distributions do not draw from it, so adding one does
            not change these draws either. None when nothing is drawn at random.
        seed (Optional[int]): The seed the sequences come from, or None.
    """

    def __init__(
        self, rng: Optional[np.random.Generator] = None, *, seed: Optional[int] = None
    ):
        """
        Initializes the Sampler.

        With neither `rng` nor `seed`, nothing is drawn at random: each sample is
        the distribution's mean (for `truncated_normal`, its mean or `low`, whichever
        is higher).

        Args:
            rng: A seeded NumPy generator, such as `np.random.default_rng(42)`. Its
                seed gives each distribution its sequence, so `Sampler(rng=
                np.random.default_rng(42))` draws the same numbers as
                `Sampler(seed=42)`, and the generator itself stays the run's main
                generator.
            seed: The run's seed, instead of `rng`.

        Raises:
            ValueError: If both `rng` and `seed` are given.
        """
        if rng is not None and seed is not None:
            raise ValueError("give rng or seed, not both")
        if seed is not None:
            rng = np.random.default_rng(seed)
        self.rng = rng
        self._seed_sequence: Optional[np.random.SeedSequence] = None
        if rng is not None:
            sequence = rng.bit_generator.seed_seq
            if isinstance(sequence, np.random.SeedSequence):
                self._seed_sequence = sequence
        self.seed: Optional[int] = (
            None if self._seed_sequence is None else self._seed_sequence.entropy  # type: ignore[assignment]
        )
        self._generators: Dict[str, np.random.Generator] = {}

    def generator(self, name: str) -> Optional[np.random.Generator]:
        """
        Returns the random sequence called `name`, made from the run's seed.

        Distributions use their registry path without the sim id, and probability
        routing uses `route.<task>`. Your own code can take a sequence of its own
        the same way, so its draws do not shift when the model changes.

        Args:
            name: The sequence's name.

        Returns:
            Optional[np.random.Generator]: The same generator for the same name every
                time; None when the sampler draws nothing at random. A sampler made
                from a generator without a seed sequence returns that generator.
        """
        if self._seed_sequence is None:
            return self.rng
        generator = self._generators.get(name)
        if generator is None:
            base = self._seed_sequence
            sequence = np.random.SeedSequence(
                base.entropy,
                spawn_key=tuple(base.spawn_key) + (zlib.crc32(name.encode()),),
                pool_size=base.pool_size,
            )
            generator = np.random.default_rng(sequence)
            self._generators[name] = generator
        return generator

    @property
    def streams(self) -> List[str]:
        """
        The names of the random sequences drawn from so far, sorted.

        Two runs that compare designs with common random numbers should list the
        same names; a name only one lists is a sequence the other never used.
        """
        return sorted(self._generators)

    def sample(self, config: DistributionConfig, min_delay: float = MIN_DELAY) -> float:
        """
        Draws a random sample using the current parameters in the provided config.

        A config the registry has registered draws from its own sequence; one built
        by hand and never registered draws from `rng`.

        Args:
            config (DistributionConfig): The live configuration object retrieved from the Registry.
            min_delay (float): The smallest value returned.

        Returns:
            float: A sampled float representing a time duration (e.g., arrival gap or service time).
                   Returns `min_delay` if the sample is smaller than `min_delay`.
                   A std of 0 gives the mean every time.

        Raises:
            ValueError: If the `dist` type string is not supported.
        """
        # The parameters were checked by DistributionConfig and by the registry on
        # every live update, so the ones each family uses are set and in range.
        family = get_distribution(config.dist)
        if self.rng is None:
            return max(min_delay, family.fixed(config))
        if not family.random:
            return max(min_delay, family.draw(config, self.rng))
        rng = self.rng if config.stream is None else self.generator(config.stream)
        return max(min_delay, family.draw(config, rng))  # type: ignore[arg-type]
