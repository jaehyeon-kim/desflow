"""Arrival rates, and the effective mean and variance of a distribution."""

from typing import Dict, List, Tuple

from desflow.config import MIN_DELAY
from desflow.explain.models import VERIFY_HINT, ArrivalResult, Graph, Node


def moments(parameters: Dict) -> Tuple[float, float]:
    """The mean and variance of the values the sampler gives for a distribution.

    The sampler raises every draw below its minimum delay to it, so a normal's mean
    is the clamped normal's (`clamped_normal_moments`), and a constant below the
    minimum delay is the minimum delay.

    Args:
        parameters: The distribution, such as `{"dist": "exponential", "rate": 2}`.

    Returns:
        Tuple[float, float]: The effective mean and variance.
    """
    from desflow.core.distributions import clamped_normal_moments, get_distribution
    from desflow.models.params import DistributionConfig

    config = DistributionConfig(**parameters)
    if config.dist == "normal":
        assert config.mean is not None
        return clamped_normal_moments(config.mean, config.std or 0.0)
    family = get_distribution(config.dist)
    mean, variance = family.mean(config), family.variance(config)
    if variance == 0 and mean < MIN_DELAY:
        mean = MIN_DELAY
    return mean, variance


def arrival_results(graph: Graph) -> List[ArrivalResult]:
    """Each arrival's rate, 1 / its effective mean gap, and its gaps' ca².

    Args:
        graph: The graph.

    Returns:
        List[ArrivalResult]: One per arrival, a replayed one unknown.
    """
    results = []
    for node in graph.nodes:
        if node.kind != "arrival":
            continue
        if node.distribution is None:
            results.append(
                ArrivalResult(name=node.label, reason="replayed rows" + VERIFY_HINT)
            )
            continue
        mean, variance = moments(node.distribution)
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
