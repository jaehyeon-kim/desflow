"""The registry of distributions: every family desflow can sample, in one place.

Each `Distribution` declares its parameters, the rules they must follow, how to draw
from it, its mean and variance, and what it gives with no random generator.
`check_distribution`, the sampler, the registry's live paths, the builder and the YAML
models all read `DISTRIBUTIONS`, so a family added here is accepted everywhere at once.

The functions take the configuration object (a `DistributionConfig`) and read its
parameters as attributes, so a draw builds no mapping.
"""

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

import numpy as np

# Parameters measured in seconds. A YAML blueprint accepts a duration such as
# `5 min` for each of them; `rate`, `shape`, `weights` and `counts` are plain numbers.
TIME_PARAMETERS = ("mean", "std", "low", "mode", "high", "scale", "value")
TIME_LIST_PARAMETERS = ("values", "edges")
# Parameters that hold a list. They are taken at load and get no registry path.
LIST_PARAMETERS = ("values", "weights", "edges", "counts")


@dataclass(frozen=True)
class Distribution:
    """One distribution family and everything desflow needs to know about it.

    Attributes:
        name: The value of `dist`, such as `uniform`.
        summary: One sentence on what the family models.
        parameters: Every parameter the family takes, required or not.
        required: The parameters that must be given, or that have a default.
        defaults: Values for parameters left out, such as a `std` of 0.
        rules: The range rules, as a reader would write them.
        check: Raises `ValueError` when a parameter breaks a rule.
        draw: Draws one value with a NumPy generator.
        mean: The distribution's mean.
        variance: The distribution's variance.
        fixed: The value given when the sampler has no random generator.
        random: Whether a draw uses the generator; `constant` does not.
    """

    name: str
    summary: str
    parameters: Tuple[str, ...]
    required: Tuple[str, ...]
    rules: str
    check: Callable[[Any], None]
    draw: Callable[[Any, np.random.Generator], float]
    mean: Callable[[Any], float]
    variance: Callable[[Any], float]
    fixed: Callable[[Any], float]
    defaults: Mapping[str, float] = field(default_factory=dict)
    random: bool = True

    @property
    def live(self) -> Tuple[str, ...]:
        """The parameters that get a registry path and can change during a run."""
        return tuple(p for p in self.parameters if p not in LIST_PARAMETERS)


def _require(config: Any, *names: str) -> None:
    for name in names:
        if getattr(config, name) is None:
            raise ValueError(f"{config.dist} needs {name}")


def _positive(config: Any, *names: str) -> None:
    for name in names:
        value = getattr(config, name)
        if value is None or not value > 0:
            raise ValueError(
                f"{config.dist} {name} must be greater than 0, got {value}"
            )


def _not_negative(config: Any, *names: str) -> None:
    for name in names:
        value = getattr(config, name)
        if value is None or not value >= 0:
            raise ValueError(f"{config.dist} {name} must be 0 or more, got {value}")


# --- exponential -------------------------------------------------------------


def _check_exponential(c: Any) -> None:
    _positive(c, "rate")


# --- normal and lognormal ----------------------------------------------------


def _check_normal(c: Any) -> None:
    _require(c, "mean")
    _not_negative(c, "std")


def _check_lognormal(c: Any) -> None:
    _require(c, "mean")
    _positive(c, "mean")
    _not_negative(c, "std")


def _draw_lognormal(c: Any, rng: np.random.Generator) -> float:
    m, s = c.mean, c.std
    mu = np.log(m**2 / np.sqrt(s**2 + m**2))
    sigma = np.sqrt(np.log(1 + (s**2 / m**2)))
    value = rng.lognormal(mu, sigma)
    # With a std of 0 the draw is the mean up to rounding, so return the mean
    # exactly. The draw is still made, so later draws stay the same.
    return m if s == 0 else float(value)


# --- truncated normal --------------------------------------------------------


def _check_truncated_normal(c: Any) -> None:
    _require(c, "mean")
    _not_negative(c, "std")
    _require(c, "low")
    if c.std == 0 and c.mean < c.low:
        raise ValueError(
            f"truncated_normal with std 0 needs a mean of at least low "
            f"{c.low}, got {c.mean}"
        )


def _draw_truncated_normal(c: Any, rng: np.random.Generator) -> float:
    if c.std == 0:
        return max(c.mean, c.low)
    return c.low + c.std * _standard_normal_above((c.low - c.mean) / c.std, rng)


def _standard_normal_above(a: float, rng: np.random.Generator) -> float:
    """Draws a standard normal value conditioned on being at least `a`, minus `a`.

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


def _truncated_moments(c: Any) -> Tuple[float, float]:
    if c.std == 0:
        return max(c.mean, c.low), 0.0
    a = (c.low - c.mean) / c.std
    density = math.exp(-a * a / 2) / math.sqrt(2 * math.pi)
    tail = 1 - 0.5 * (1 + math.erf(a / math.sqrt(2)))
    ratio = density / tail
    mean = c.mean + c.std * ratio
    variance = c.std**2 * (1 + a * ratio - ratio**2)
    return mean, variance


# --- constant ----------------------------------------------------------------


def _check_constant(c: Any) -> None:
    _not_negative(c, "value")


# --- uniform and triangular --------------------------------------------------


def _check_uniform(c: Any) -> None:
    _require(c, "low", "high")
    if not c.low < c.high:
        raise ValueError(f"uniform needs low < high, got low {c.low}, high {c.high}")


def _check_triangular(c: Any) -> None:
    _require(c, "low", "mode", "high")
    if not c.low < c.high:
        raise ValueError(f"triangular needs low < high, got low {c.low}, high {c.high}")
    if not c.low <= c.mode <= c.high:
        raise ValueError(
            f"triangular needs low <= mode <= high, got low {c.low}, mode {c.mode}, "
            f"high {c.high}"
        )


def _triangular_variance(c: Any) -> float:
    lo, mo, hi = c.low, c.mode, c.high
    return (lo * lo + mo * mo + hi * hi - lo * mo - lo * hi - mo * hi) / 18


# --- gamma and weibull -------------------------------------------------------


def _check_shape_scale(c: Any) -> None:
    _positive(c, "shape", "scale")


def _weibull_mean(c: Any) -> float:
    return c.scale * math.gamma(1 + 1 / c.shape)


def _weibull_variance(c: Any) -> float:
    first = math.gamma(1 + 1 / c.shape)
    return c.scale**2 * (math.gamma(1 + 2 / c.shape) - first**2)


# --- empirical ---------------------------------------------------------------


def _check_empirical(c: Any) -> None:
    from_values = c.values is not None or c.weights is not None
    from_histogram = c.edges is not None or c.counts is not None
    if from_values == from_histogram:
        raise ValueError(
            "empirical needs values (with optional weights) or a histogram "
            "(edges and counts), not both and not neither"
        )
    if from_values:
        if not c.values:
            raise ValueError("empirical needs at least one value")
        if c.weights is not None:
            if len(c.weights) != len(c.values):
                raise ValueError(
                    f"empirical needs one weight per value, got {len(c.weights)} "
                    f"weights for {len(c.values)} values"
                )
            _check_frequencies("weights", c.weights)
        return
    if c.edges is None or c.counts is None:
        raise ValueError("an empirical histogram needs both edges and counts")
    if len(c.edges) < 2:
        raise ValueError("an empirical histogram needs at least two edges")
    if any(not low < high for low, high in zip(c.edges, c.edges[1:])):
        raise ValueError("empirical edges must increase from one to the next")
    if len(c.counts) != len(c.edges) - 1:
        raise ValueError(
            f"empirical needs one count per bin, so {len(c.edges) - 1} counts for "
            f"{len(c.edges)} edges, got {len(c.counts)}"
        )
    _check_frequencies("counts", c.counts)


def _check_frequencies(name: str, numbers: Any) -> None:
    if any(not n >= 0 for n in numbers):
        raise ValueError(f"empirical {name} must be 0 or more")
    if not sum(numbers) > 0:
        raise ValueError(f"empirical {name} must add up to more than 0")


def _probabilities(numbers: Any) -> np.ndarray:
    weights = np.asarray(numbers, dtype=float)
    return weights / weights.sum()


def _draw_empirical(c: Any, rng: np.random.Generator) -> float:
    if c.values is not None:
        if c.weights is None:
            return float(c.values[rng.integers(len(c.values))])
        index = rng.choice(len(c.values), p=_probabilities(c.weights))
        return float(c.values[index])
    # A histogram: a bin chosen by its count, then a uniform point inside it
    index = rng.choice(len(c.counts), p=_probabilities(c.counts))
    return float(rng.uniform(c.edges[index], c.edges[index + 1]))


def _empirical_moments(c: Any) -> Tuple[float, float]:
    if c.values is not None:
        values = np.asarray(c.values, dtype=float)
        p = (
            np.full(len(values), 1 / len(values))
            if c.weights is None
            else _probabilities(c.weights)
        )
        mean = float(np.sum(p * values))
        return mean, float(np.sum(p * (values - mean) ** 2))
    edges = np.asarray(c.edges, dtype=float)
    p = _probabilities(c.counts)
    low, high = edges[:-1], edges[1:]
    middle = (low + high) / 2
    mean = float(np.sum(p * middle))
    second = float(np.sum(p * ((high - low) ** 2 / 12 + middle**2)))
    return mean, second - mean**2


def _mean_of(c: Any) -> float:
    return c.mean


def _distributions(*families: Distribution) -> Dict[str, Distribution]:
    return {family.name: family for family in families}


DISTRIBUTIONS: Dict[str, Distribution] = _distributions(
    Distribution(
        name="exponential",
        summary="Gaps between independent arrivals; the memoryless distribution.",
        parameters=("rate",),
        required=("rate",),
        rules="rate > 0, in events per second",
        check=_check_exponential,
        draw=lambda c, rng: float(rng.exponential(1.0 / c.rate)),
        mean=lambda c: 1.0 / c.rate,
        variance=lambda c: 1.0 / c.rate**2,
        fixed=lambda c: 1.0 / c.rate,
    ),
    Distribution(
        name="normal",
        summary="Symmetric times around a mean. Draws below 0.00001 s (`min_delay`) "
        "are raised to it, which raises the mean; truncated_normal avoids that.",
        parameters=("mean", "std"),
        required=("mean", "std"),
        defaults={"std": 0.0},
        rules="std >= 0; a std of 0 gives the mean every time",
        check=_check_normal,
        draw=lambda c, rng: float(rng.normal(c.mean, c.std)),
        mean=_mean_of,
        variance=lambda c: c.std**2,
        fixed=_mean_of,
    ),
    Distribution(
        name="lognormal",
        summary="Positive times with a long right tail, given by their mean and std.",
        parameters=("mean", "std"),
        required=("mean", "std"),
        defaults={"std": 0.0},
        rules="mean > 0, std >= 0",
        check=_check_lognormal,
        draw=_draw_lognormal,
        mean=_mean_of,
        variance=lambda c: c.std**2,
        fixed=_mean_of,
    ),
    Distribution(
        name="truncated_normal",
        summary="A normal that draws again below low instead of clamping.",
        parameters=("mean", "std", "low"),
        required=("mean", "std", "low"),
        defaults={"std": 0.0, "low": 0.0},
        rules="std >= 0; low is 0 when not given; with std 0, mean >= low",
        check=_check_truncated_normal,
        draw=_draw_truncated_normal,
        mean=lambda c: _truncated_moments(c)[0],
        variance=lambda c: _truncated_moments(c)[1],
        fixed=lambda c: max(c.mean, c.low),
    ),
    Distribution(
        name="constant",
        summary="The same time every draw, such as a conveyor or a set-up.",
        parameters=("value",),
        required=("value",),
        rules="value >= 0",
        check=_check_constant,
        draw=lambda c, rng: c.value,
        mean=lambda c: c.value,
        variance=lambda c: 0.0,
        fixed=lambda c: c.value,
        random=False,
    ),
    Distribution(
        name="uniform",
        summary="Every time between low and high equally likely.",
        parameters=("low", "high"),
        required=("low", "high"),
        rules="low < high",
        check=_check_uniform,
        draw=lambda c, rng: float(rng.uniform(c.low, c.high)),
        mean=lambda c: (c.low + c.high) / 2,
        variance=lambda c: (c.high - c.low) ** 2 / 12,
        fixed=lambda c: (c.low + c.high) / 2,
    ),
    Distribution(
        name="triangular",
        summary="An expert's estimate: the shortest, the most likely and the "
        "longest time.",
        parameters=("low", "mode", "high"),
        required=("low", "mode", "high"),
        rules="low <= mode <= high, low < high",
        check=_check_triangular,
        draw=lambda c, rng: float(rng.triangular(c.low, c.mode, c.high)),
        mean=lambda c: (c.low + c.mode + c.high) / 3,
        variance=_triangular_variance,
        fixed=lambda c: (c.low + c.mode + c.high) / 3,
    ),
    Distribution(
        name="gamma",
        summary="Times less variable than exponential. A whole-number shape is "
        "the Erlang distribution: that many exponential steps in a row.",
        parameters=("shape", "scale"),
        required=("shape", "scale"),
        rules="shape > 0, scale > 0; mean = shape * scale",
        check=_check_shape_scale,
        draw=lambda c, rng: float(rng.gamma(c.shape, c.scale)),
        mean=lambda c: c.shape * c.scale,
        variance=lambda c: c.shape * c.scale**2,
        fixed=lambda c: c.shape * c.scale,
    ),
    Distribution(
        name="weibull",
        summary="Time to failure: a shape below 1 fails early, above 1 wears out.",
        parameters=("shape", "scale"),
        required=("shape", "scale"),
        rules="shape > 0, scale > 0",
        check=_check_shape_scale,
        draw=lambda c, rng: float(c.scale * rng.weibull(c.shape)),
        mean=_weibull_mean,
        variance=_weibull_variance,
        fixed=_weibull_mean,
    ),
    Distribution(
        name="empirical",
        summary="Times taken from real data: listed values with optional weights, "
        "or a histogram of edges and counts. Taken at load; they do not change live.",
        parameters=("values", "weights", "edges", "counts"),
        required=(),
        rules="values, or edges and counts; one weight per value and one count "
        "per bin, each 0 or more and adding up to more than 0; edges increasing",
        check=_check_empirical,
        draw=_draw_empirical,
        mean=lambda c: _empirical_moments(c)[0],
        variance=lambda c: _empirical_moments(c)[1],
        fixed=lambda c: _empirical_moments(c)[0],
    ),
)

# Every parameter any family takes, in the order DistributionConfig declares them.
PARAMETERS: Tuple[str, ...] = tuple(
    dict.fromkeys(p for family in DISTRIBUTIONS.values() for p in family.parameters)
)


def get_distribution(name: Optional[str]) -> Distribution:
    """Returns the registered family called `name`.

    Args:
        name: The value of `dist`.

    Returns:
        Distribution: The family.

    Raises:
        ValueError: If no family has that name. The message lists the names.
    """
    family = DISTRIBUTIONS.get(name)  # type: ignore[arg-type]
    if family is None:
        raise ValueError(
            f"Unsupported distribution '{name}'. Use one of: {', '.join(DISTRIBUTIONS)}"
        )
    return family
