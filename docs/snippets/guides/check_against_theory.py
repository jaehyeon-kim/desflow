"""Check Against Theory: a shop the queueing formulas describe, checked against them.

check_against_theory.yaml is one packer with exponential gaps and packing times. The
script works out its wait by hand, verifies the model against `explain`'s analysis
from Python, and shows a shop the formulas do not cover, with the reason.
"""

import math
from pathlib import Path

from desflow import SimulationContext

BLUEPRINT = Path(__file__).resolve().parents[1] / "yaml" / "check_against_theory.yaml"


def by_hand(packers: int, rate: float, service: float = 1.0) -> float:
    """The mean wait in the queue from the M/M/1 and M/M/c formulas."""
    # --8<-- [start:by-hand]
    load = rate * service  # packers' worth of work arriving each second
    utilisation = load / packers
    if packers == 1:
        return utilisation / (1 - utilisation) * service  # M/M/1
    # M/M/c: Erlang C, the chance that every packer is busy, over (c / service - rate)
    last = load**packers / math.factorial(packers) / (1 - utilisation)
    busy = last / (sum(load**k / math.factorial(k) for k in range(packers)) + last)
    return busy / (packers / service - rate)
    # --8<-- [end:by-hand]


def verify():
    """The blueprint's one packer, verified with 10 runs from Python."""
    # --8<-- [start:verify]
    app = SimulationContext.from_yaml(BLUEPRINT)
    explanation = app.analyze(verify=True, replications=10)
    for c in explanation.verification.comparisons:
        result = "differs" if c.differs else "agrees"
        print(
            f"{c.node} {c.measure}: analysis {c.analytical:.3f}, "
            f"runs {c.measured:.3f} ± {c.half_width:.3f}, {result}"
        )
    # --8<-- [end:verify]
    return explanation


def unsupported():
    """Two packers whose packing always takes 1 second: no formula for the wait."""
    # --8<-- [start:unsupported]
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=1.5, spawn="pack_order")
    app.add_service("packing", dist="constant", value=1)
    app.add_resource("packers", current_cap=2, max_cap=4)
    app.task("packing", "packers", name="pack_order")(lambda task_id: {})
    packers = app.analyze().resources[0]
    print(f"utilisation {packers.utilization}, status {packers.status}")
    print(f"wait: {packers.wait_reason}")
    # --8<-- [end:unsupported]
    return packers


if __name__ == "__main__":
    print(f"one packer, an order every 1.33 s (rate 0.75): {by_hand(1, 0.75):.3f} s")
    print(f"two packers at 1.5 orders a second: {by_hand(2, 1.5):.3f} s")
    verify()
    unsupported()
