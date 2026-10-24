"""The units a resource allows, and the hints that bring it to a target utilisation."""

import math
from typing import List, Optional

from desflow.explain.formulas import CONGESTED_AT, UNSTABLE_AT, status_of
from desflow.explain.models import CapacityRow, Hint, ResourceResult

# Up to this many units, every number of units gets a row of its own
_ALL_ROWS = 20


def capacity_rows(work: float, current: Optional[int], most: int) -> List[CapacityRow]:
    """ρ and the status at each number of units the resource allows.

    Up to 20 units every number gets a row; above that, only the telling ones: the
    first stable number, the first below 0.85, the current units and `max_cap`.

    Args:
        work: λ τ, the units' worth of work arriving.
        current: The current units.
        most: `max_cap`.

    Returns:
        List[CapacityRow]: The rows, by units.
    """
    return [
        CapacityRow(capacity=c, utilization=work / c, status=status_of(work / c))
        for c in _rows(work, current, most)
    ]


def _rows(work: float, current: Optional[int], most: int) -> List[int]:
    if most <= _ALL_ROWS:
        return list(range(1, most + 1))
    stable = math.floor(work / UNSTABLE_AT) + 1
    below = math.floor(work / CONGESTED_AT) + 1
    chosen = {
        c for c in (stable, below, current, most) if c is not None and 1 <= c <= most
    }
    return sorted(chosen)


def _units(count: int) -> str:
    return f"{count} unit" + ("s" if count != 1 else "")


def _requests(rate: float) -> str:
    """A rate in a form a reader can picture: below 1 a second, the gap between
    requests and a count a minute, with the rate beside them."""
    if rate >= 1:
        shown = f"{rate:.4g}"
        return f"{shown} request{'' if shown == '1' else 's'} a second"
    gap = f"{round(1 / rate, 2):g}"
    return (
        f"a request every {gap} second{'' if gap == '1' else 's'} "
        f"({round(rate * 60, 2):g} a minute, {rate:.4g} a second)"
    )


def hints(resource: ResourceResult, target: float) -> List[Hint]:
    """The units that bring the resource to `target`, and the rate into it that does
    at its current units: at most the target below 1, at least it from 1.

    Args:
        resource: The resource's result.
        target: The utilisation to aim for.

    Returns:
        List[Hint]: Empty when the rate into the resource is unknown.
    """
    if resource.rate is None or not resource.service_mean or resource.servers is None:
        return []
    work = resource.rate * resource.service_mean
    name, found = resource.name, []
    if target < 1:
        units = max(1, math.ceil(work / target - 1e-9))
        found.append(
            Hint(
                resource=name,
                kind="servers",
                value=units,
                text=f"{name} with {_units(units)}: utilisation {work / units:.3g}, at most {target:g}",
            )
        )
    else:
        units = math.floor(work / target + 1e-9)
        if units >= 1:
            found.append(
                Hint(
                    resource=name,
                    kind="servers",
                    value=units,
                    text=f"{name} with {_units(units)}: utilisation {work / units:.3g}, at least {target:g}, for deliberate overload",
                )
            )
    if resource.servers >= 1:
        rate = target * resource.servers / resource.service_mean
        found.append(
            Hint(
                resource=name,
                kind="rate",
                value=rate,
                text=f"{name} with {_units(resource.servers)}: utilisation {target:g} at {_requests(rate)}",
            )
        )
    return found
