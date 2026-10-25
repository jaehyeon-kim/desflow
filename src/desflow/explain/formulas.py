"""The queueing formulas: Kingman's wait, Erlang C, and the status thresholds.

Sources, read 24 Oct 2026: Kingman's formula (Wikipedia, Kingman's formula) for one
server with a renewal input; Erlang C and the M/M/c mean response time (Wikipedia,
M/M/c queue), of which the wait in the queue is the response time less the mean
service time.
"""

import math

from desflow.explain.models import Status

CONGESTED_AT = 0.85
UNSTABLE_AT = 1.0


def kingman_wait(
    utilization: float, ca2: float, cs2: float, service_mean: float
) -> float:
    """Kingman's mean wait in the queue of one server: (ρ / (1 − ρ)) ((ca² + cs²) / 2) τ.

    With exponential gaps and service (ca² = cs² = 1) it is the M/M/1 wait, and with
    exponential gaps alone the Pollaczek-Khinchine M/G/1 wait.

    Args:
        utilization: ρ, below 1.
        ca2: The squared coefficient of variation of the gaps.
        cs2: The squared coefficient of variation of the service time.
        service_mean: τ, the mean service time.

    Returns:
        float: The mean wait in the queue.
    """
    return utilization / (1 - utilization) * (ca2 + cs2) / 2 * service_mean


def erlang_c(servers: int, load: float) -> float:
    """The chance that an arrival finds every server busy in an M/M/c queue.

    Args:
        servers: c.
        load: λ / μ, the work arriving in servers' worth, below `servers`.

    Returns:
        float: C(c, λ / μ).
    """
    last = load**servers / math.factorial(servers) / (1 - load / servers)
    return last / (sum(load**k / math.factorial(k) for k in range(servers)) + last)


def erlang_wait(servers: int, rate: float, service_mean: float) -> float:
    """The mean wait in the queue of an M/M/c queue: C(c, λ / μ) / (c μ − λ).

    Args:
        servers: c.
        rate: λ.
        service_mean: τ = 1 / μ.

    Returns:
        float: The mean wait in the queue.
    """
    load = rate * service_mean
    return erlang_c(servers, load) / (servers / service_mean - rate)


def status_of(utilization: float) -> Status:
    """`OK`, `CONGESTED` from 0.85, `UNSTABLE` from 1.

    Args:
        utilization: ρ.

    Returns:
        Status: The status.
    """
    if utilization >= UNSTABLE_AT:
        return "UNSTABLE"
    return "CONGESTED" if utilization >= CONGESTED_AT else "OK"
