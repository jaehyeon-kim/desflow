"""The tables `render_terminal` prints: the resources, the units each allows, and
the verify comparison."""

from typing import Optional

from rich import box
from rich.table import Table

from desflow.explain.models import Explanation, ResourceResult


def _number(value: Optional[float]) -> str:
    return "-" if value is None else f"{value:.4g}"


def resources(explanation: Explanation) -> Table:
    """The resources: λ, μ, c, ρ, Wq, L and the status of each.

    Args:
        explanation: The analysis.

    Returns:
        Table: The table.
    """
    table = Table(title="Resources", box=box.SIMPLE_HEAD, title_justify="left")
    for column in ("resource", "λ", "μ", "c", "ρ", "Wq", "L", "status"):
        table.add_column(
            column, justify="left" if column in ("resource", "status") else "right"
        )
    for r in explanation.resources:
        units = "no limit" if r.servers is None else str(r.servers)
        table.add_row(
            r.name,
            _number(r.rate),
            _number(r.service_rate),
            units,
            _number(r.utilization),
            _number(r.wait),
            _number(r.in_system),
            r.status or "-",
        )
    return table


def capacities(resource: ResourceResult) -> Table:
    """ρ and the status at each number of units the resource allows.

    Args:
        resource: The resource's result.

    Returns:
        Table: The table.
    """
    table = Table(
        title=f"{resource.name} by units", box=box.SIMPLE_HEAD, title_justify="left"
    )
    for column in ("c", "ρ", "status"):
        table.add_column(column, justify="right" if column != "status" else "left")
    for row in resource.capacities:
        table.add_row(str(row.capacity), _number(row.utilization), row.status)
    return table


def comparisons(explanation: Explanation) -> Table:
    """The verify table: each measure's analytical and measured values.

    Args:
        explanation: The analysis, with its verification.

    Returns:
        Table: The table.
    """
    verification = explanation.verification
    assert verification is not None
    runs = verification.replications or 1
    table = Table(
        title=f"Verify: {runs} run{'s' if runs > 1 else ''} to {verification.until:g} s, seed {verification.seed}",
        box=box.SIMPLE_HEAD,
        title_justify="left",
    )
    for column in ("node", "measure", "analysis", "measured", "±", "result"):
        table.add_column(
            column,
            justify="left" if column in ("node", "measure", "result") else "right",
        )
    for c in verification.comparisons:
        if c.differs is None:
            result = "no reading" if c.analytical is not None else "not compared"
        else:
            result = "differs" if c.differs else "agrees"
        table.add_row(
            c.node,
            c.measure,
            _number(c.analytical),
            _number(c.measured),
            _number(c.half_width),
            result,
        )
    return table
