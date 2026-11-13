"""Arrival rates, and the effective mean and variance of a distribution."""

import math
from typing import Dict, List, Tuple

from desflow.config import MIN_DELAY
from desflow.explain import features
from desflow.explain.models import VERIFY_HINT, ArrivalResult, Graph, Node


_NO_RATE = "a gap of 0 has no rate"


def moments(parameters: Dict, owner: str) -> Tuple[float, float]:
    """The mean and variance of the values the sampler gives for a distribution.

    The sampler raises every negative draw to 0, so a normal's mean is the clamped
    normal's (`clamped_normal_moments`), and a negative constant is 0.

    Args:
        parameters: The distribution, such as `{"dist": "exponential", "rate": 2}`.
        owner: The arrival or task it belongs to, such as `arrival orders`, for the
            error.

    Returns:
        Tuple[float, float]: The effective mean and variance.

    Raises:
        ValueError: If the mean or the variance, or the square of the mean that the
            callers divide by, is too large for a float, as a rate of 1e-200 gives.
    """
    from desflow.models.distributions import clamped_normal_moments, get_distribution
    from desflow.models.params import DistributionConfig

    config = DistributionConfig(**parameters)
    try:
        if config.dist == "normal":
            assert config.mean is not None
            mean, variance = clamped_normal_moments(config.mean, config.std or 0.0)
        else:
            family = get_distribution(config.dist)
            mean, variance = family.mean(config), family.variance(config)
        # Beyond the float range, integer arithmetic raises OverflowError and float
        # arithmetic gives inf; both are refused below
        finite = all(map(math.isfinite, (mean, variance, mean * mean)))
    except ArithmeticError:
        finite = False
    if not finite:
        options = ", ".join(f"{k} {v}" for k, v in parameters.items() if k != "dist")
        raise ValueError(
            f"{owner}: {parameters['dist']} with {options} has a mean or variance "
            f"too large for a float"
        )
    if variance == 0 and mean < MIN_DELAY:
        mean = MIN_DELAY
    return mean, variance


def arrival_results(graph: Graph) -> List[ArrivalResult]:
    """Each arrival's rate, 1 / its effective mean gap, and its gaps' ca².

    Args:
        graph: The graph.

    Returns:
        List[ArrivalResult]: One per arrival; a replayed one, one with computed
            gaps and one with `initial` alone are unknown.
    """
    results = []
    for node in graph.nodes:
        if node.kind != "arrival":
            continue
        if node.distribution is None:
            what = "replayed rows"
            if features.COMPUTED_GAPS in node.features:
                what = "computed gaps"
            elif features.INITIAL_ONLY in node.features:
                what = "it starts only its initial entities, at the start of the run"
            results.append(ArrivalResult(name=node.label, reason=what + VERIFY_HINT))
            continue
        if features.GROUPED_ARRIVALS in node.features:
            results.append(
                ArrivalResult(
                    name=node.label,
                    reason="group starts a batch at each arrival, which is not a "
                    "Poisson input" + VERIFY_HINT,
                )
            )
            continue
        if features.INACTIVE_ARRIVAL in node.features:
            results.append(
                ArrivalResult(
                    name=node.label,
                    reason="it starts off (active: false), so its rate holds only "
                    "once something turns it on" + VERIFY_HINT,
                )
            )
            continue
        if features.LIMITED_ARRIVALS in node.features:
            results.append(
                ArrivalResult(
                    name=node.label,
                    reason="count, keys, key or max_events change how many entities "
                    "arrive" + VERIFY_HINT,
                )
            )
            continue
        mean, variance = moments(node.distribution, f"arrival {node.label}")
        if mean == 0:
            results.append(ArrivalResult(name=node.label, reason=_NO_RATE))
            continue
        results.append(
            ArrivalResult(name=node.label, rate=1.0 / mean, ca2=variance / mean**2)
        )
    return results


def exponential(node: Node) -> bool:
    """True for an arrival or task whose times are exponential.

    Args:
        node: An arrival or task node.

    Returns:
        bool: Whether its distribution is exponential.
    """
    return node.distribution is not None and node.distribution["dist"] == "exponential"
