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

from desflow.config import MIN_DELAY

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
        from_moments: The parameters that give a mean and a standard deviation,
            or None when no simple formula gives them. It raises `ValueError`
            when the family cannot have that pair.
        no_formula: Why `from_moments` is None.
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
    from_moments: Optional[Callable[[float, float], Dict[str, float]]] = None
    no_formula: str = ""

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


# --- parameters from a mean and a standard deviation -------------------------

_NO_FORMULA = "no simple formula gives the parameters from a mean and std"


def _exponential_from(m: float, s: float) -> Dict[str, float]:
    if not math.isclose(s, m):
        raise ValueError(
            f"an exponential's std equals its mean, so mean {m:g} needs std {m:g}, "
            f"not {s:g}; use gamma for another std"
        )
    return {"rate": 1 / m}


def _mean_std(m: float, s: float) -> Dict[str, float]:
    return {"mean": m, "std": s}


def _constant_from(m: float, s: float) -> Dict[str, float]:
    if s != 0:
        raise ValueError(f"a constant has std 0, not {s:g}")
    return {"value": m}


def _uniform_from(m: float, s: float) -> Dict[str, float]:
    half = math.sqrt(3) * s
    if m - half < 0:
        raise ValueError(
            f"a uniform with mean {m:g} and std {s:g} has low {m - half:.6g}, "
            "below 0, and a time cannot be negative; give a smaller std"
        )
    return {"low": m - half, "high": m + half}


def _gamma_from(m: float, s: float) -> Dict[str, float]:
    if s == 0:
        raise ValueError("a gamma needs a std above 0; use constant for std 0")
    return {"shape": (m / s) ** 2, "scale": s * s / m}


def parameters_from_moments(
    family: "Distribution", mean: float, std: float
) -> Dict[str, float]:
    """Returns the parameters of `family` that give this mean and standard deviation.

    The mean must be above 0, except for `constant`, whose value 0 gives a time
    of 0 on every draw. The result is checked against the family's rules, so a pair the family cannot have is
    refused.

    Args:
        family: The distribution family.
        mean: The mean, in seconds.
        std: The standard deviation, in seconds.

    Returns:
        Dict[str, float]: The family's parameters.

    Raises:
        ValueError: If no simple formula gives the family's parameters (the
            message is the family's `no_formula`), the std is negative, the mean
            is not above 0 for a family other than `constant`, or the family
            cannot have this pair.
    """
    if family.from_moments is None:
        raise ValueError(f"{family.name}: {family.no_formula}")
    if not std >= 0:
        raise ValueError(f"std must be 0 or more, got {std:g}")
    if family.name != "constant" and not mean > 0:
        raise ValueError(
            f"mean must be above 0 for {family.name}, got {mean:g}; for a time of 0 "
            "on every draw, use constant with value 0"
        )
    parameters = family.from_moments(mean, std)
    from desflow.models.params import DistributionConfig

    DistributionConfig(dist=family.name, **parameters)  # type: ignore[arg-type]
    return parameters


def clamped_normal_moments(
    mean: float, std: float, floor: float = MIN_DELAY
) -> Tuple[float, float]:
    """Returns the mean and variance of a normal's draws once the sampler has
    raised every draw below `floor` to it.

    These are the rectified normal formulas, with the clamp at `floor` instead of
    0 (Rectified Gaussian distribution, Wikipedia, Mean and variance).

    Args:
        mean: The normal's mean.
        std: The normal's standard deviation.
        floor: The value draws below it are raised to; the sampler's `min_delay`.

    Returns:
        Tuple[float, float]: The mean and the variance of the clamped draws.
    """
    if std == 0:
        value = max(mean, floor)
        return value, 0.0
    a = (floor - mean) / std
    density = math.exp(-a * a / 2) / math.sqrt(2 * math.pi)
    below = 0.5 * (1 + math.erf(a / math.sqrt(2)))
    first = floor * below + mean * (1 - below) + std * density
    second = (
        floor * floor * below
        + (mean * mean + std * std) * (1 - below)
        + std * (mean + floor) * density
    )
    return first, max(0.0, second - first * first)


def variability(mean: float, std: float) -> str:
    """Names how variable a time is, from its coefficient of variation std / mean.

    The scale is desflow's own, not a standard one: none at 0, low below 0.75,
    moderate from 0.75 to 1.33, where an exponential's 1 lies, and high above.
    A std of 0 is `none` whatever the mean, because every draw is the same.

    Args:
        mean: The mean.
        std: The standard deviation.

    Returns:
        str: `none`, `low`, `moderate` or `high`, or `undefined` for a mean that
            is not above 0 with a std above 0.
    """
    if std == 0:
        return "none"
    if not mean > 0:
        return "undefined"
    cv = std / mean
    if cv < 0.75:
        return "low"
    return "moderate" if cv <= 1.33 else "high"


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
        from_moments=_exponential_from,
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
        from_moments=_mean_std,
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
        from_moments=_mean_std,
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
        no_formula=_NO_FORMULA,
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
        from_moments=_constant_from,
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
        from_moments=_uniform_from,
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
        no_formula=_NO_FORMULA,
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
        from_moments=_gamma_from,
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
        no_formula=_NO_FORMULA,
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
        no_formula=_NO_FORMULA,
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
