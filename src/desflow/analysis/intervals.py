"""Confidence intervals on a mean, with the Student t quantile written out.

The quantile is computed here rather than taken from scipy, so replications and
paired comparisons work in a base install. The t distribution's cumulative
probability is the regularized incomplete beta function, evaluated by its continued
fraction, and the quantile is found by bisection on it, which converges to the
closest float in a fixed number of steps.
"""

import math
from dataclasses import dataclass
from statistics import fmean, stdev
from typing import Sequence

# Steps of the continued fraction before giving up, and the relative change at
# which it has converged. 300 steps covers every degree of freedom an experiment uses.
_MAX_TERMS = 300
_TOLERANCE = 1e-15
_TINY = 1e-300


def _beta_fraction(x: float, a: float, b: float) -> float:
    """The continued fraction of the incomplete beta function, by Lentz's method."""
    c, d = 1.0, 1.0 - (a + b) * x / (a + 1.0)
    d = 1.0 / (d if abs(d) > _TINY else _TINY)
    result = d
    for m in range(1, _MAX_TERMS + 1):
        for numerator in (
            m * (b - m) * x / ((a + 2 * m - 1) * (a + 2 * m)),
            -(a + m) * (a + b + m) * x / ((a + 2 * m) * (a + 2 * m + 1)),
        ):
            d = 1.0 + numerator * d
            d = 1.0 / (d if abs(d) > _TINY else _TINY)
            c = 1.0 + numerator / c
            c = c if abs(c) > _TINY else _TINY
            result *= c * d
        if abs(c * d - 1.0) < _TOLERANCE:
            break
    return result


def _incomplete_beta(x: float, a: float, b: float) -> float:
    """The regularized incomplete beta function I_x(a, b), for x in [0, 1]."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    log_front = (
        math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        + a * math.log(x)
        + b * math.log1p(-x)
    )
    # The fraction converges fast only on one side of the mean; the other side
    # uses the symmetry I_x(a, b) = 1 - I_(1-x)(b, a).
    if x < (a + 1.0) / (a + b + 2.0):
        return math.exp(log_front) * _beta_fraction(x, a, b) / a
    return 1.0 - math.exp(log_front) * _beta_fraction(1.0 - x, b, a) / b


def _t_cdf(t: float, df: float) -> float:
    """
    Returns the probability that a Student t variable is at most `t`.

    Args:
        t: The value.
        df: The degrees of freedom, above 0.

    Returns:
        float: The cumulative probability.
    """
    tail = 0.5 * _incomplete_beta(df / (df + t * t), df / 2.0, 0.5)
    return 1.0 - tail if t > 0 else tail


def t_quantile(p: float, df: float) -> float:
    """
    Returns the value a Student t variable is at most with probability `p`.

    Args:
        p: The probability, between 0 and 1.
        df: The degrees of freedom, above 0.

    Returns:
        float: The quantile, such as 2.262 for `p=0.975` and `df=9`.

    Raises:
        ValueError: If `p` is not between 0 and 1, or `df` is not above 0.
    """
    if not 0.0 < p < 1.0:
        raise ValueError(f"p must be between 0 and 1, got {p}")
    if not df > 0:
        raise ValueError(f"degrees of freedom must be above 0, got {df}")
    if p == 0.5:
        return 0.0
    if p < 0.5:
        return -t_quantile(1.0 - p, df)
    low, high = 0.0, 1.0
    while _t_cdf(high, df) < p:
        low, high = high, high * 2.0
    # Halving the bracket until it stops shrinking gives the closest float
    while True:
        middle = (low + high) / 2.0
        if middle in (low, high):
            return high
        if _t_cdf(middle, df) < p:
            low = middle
        else:
            high = middle


@dataclass(frozen=True)
class Interval:
    """
    A mean and the half-width of its confidence interval.

    Attributes:
        n (int): How many values the mean is of.
        mean (float): Their mean.
        std (float): Their sample standard deviation.
        half_width (float): The Student t half-width at `level`.
        level (float): The confidence level, such as 0.95.
    """

    n: int
    mean: float
    std: float
    half_width: float
    level: float

    @property
    def low(self) -> float:
        """The lower bound, `mean - half_width`."""
        return self.mean - self.half_width

    @property
    def high(self) -> float:
        """The upper bound, `mean + half_width`."""
        return self.mean + self.half_width


def interval(values: Sequence[float], level: float = 0.95) -> Interval:
    """
    Returns the mean of `values` and the Student t half-width of its interval.

    The half-width is `t × s / √n`, where `s` is the sample standard deviation and
    `t` the quantile at `(1 + level) / 2` with `n - 1` degrees of freedom.

    Args:
        values: Independent values, such as one statistic from each replication.
        level: The confidence level, between 0 and 1. Defaults to 0.95.

    Returns:
        Interval: The mean and its half-width.

    Raises:
        ValueError: If there are fewer than 2 values or the level is not between 0
            and 1.
    """
    if not 0.0 < level < 1.0:
        raise ValueError(f"level must be between 0 and 1, got {level}")
    n = len(values)
    if n < 2:
        raise ValueError(f"an interval needs at least 2 values, got {n}")
    spread = stdev(values)
    quantile = t_quantile((1.0 + level) / 2.0, n - 1)
    return Interval(
        n=n,
        mean=fmean(values),
        std=spread,
        half_width=quantile * spread / math.sqrt(n),
        level=level,
    )
