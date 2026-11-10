"""The fixed draws of the Bank Renege port (EX-1), and the SimPy program that uses them.

SimPy's `bank_renege.py` draws the gap, the patience and the time at the counter with
`random`, whose sequence a blueprint cannot reproduce. The comparison therefore fixes
all three: the same constants are written into the blueprint (`blueprint`) and passed
to the program (`reference`). The lines of the program's print output are compared with
those `render` makes from the records.

The patience is 1.5, 2 or 3: with a gap of 10 and 12 seconds at the counter, the second
customer waits 2, so 1.5 reneges and 3 does not. At 2 the patience ends at the instant
the unit is released, and SimPy and desflow give the same lines.
"""

from typing import Any, Dict, List

import simpy

GAP = 10
SERVICE = 12
CUSTOMERS = 5


def blueprint(text: str, patience: float) -> str:
    """The blueprint with its three random draws replaced by the fixed values."""
    replaced = (
        text.replace(
            "dist: exponential\n    rate: 0.1", f"dist: constant\n    value: {GAP}"
        )
        .replace(
            "patience: {gen: uniform, low: 1, high: 3}",
            f"patience: {{gen: literal, value: {patience}}}",
        )
        .replace(
            "service_time: {gen: sample, dist: exponential, rate: 0.08333333333333333}",
            f"service_time: {SERVICE}",
        )
    )
    assert replaced.count("constant") == 1 and f"service_time: {SERVICE}" in replaced
    return replaced


def reference(patience: float) -> List[str]:
    """The lines `bank_renege.py` prints, with the draws fixed."""
    env = simpy.Environment()
    counter = simpy.Resource(env, capacity=1)
    lines: List[str] = []

    def customer(name: str):
        arrive = env.now
        lines.append(f"{arrive:7.4f} {name}: Here I am")
        with counter.request() as req:
            results = yield req | env.timeout(patience)
            wait = env.now - arrive
            if req in results:
                lines.append(f"{env.now:7.4f} {name}: Waited {wait:6.3f}")
                yield env.timeout(SERVICE)
                lines.append(f"{env.now:7.4f} {name}: Finished")
            else:
                lines.append(f"{env.now:7.4f} {name}: RENEGED after {wait:6.3f}")

    def source():
        for i in range(CUSTOMERS):
            env.process(customer(f"Customer{i:02d}"))
            yield env.timeout(GAP)

    env.process(source())
    env.run()
    return lines


def render(records: List[Dict[str, Any]]) -> List[str]:
    """The same lines from a run's records."""
    arrived: Dict[str, float] = {}
    lines: List[str] = []
    for record in records:
        if record["stream_type"] != "event" or record["task"] != "customer":
            continue
        time, name, value = record["sim_ts"], record["key"], record["value"]
        status = value.get("status")
        if status == "queued":
            arrived[name] = time
            lines.append(f"{time:7.4f} {name}: Here I am")
        elif status == "started":
            lines.append(f"{time:7.4f} {name}: Waited {time - arrived[name]:6.3f}")
        elif status == "reneged":
            lines.append(
                f"{time:7.4f} {name}: RENEGED after {time - arrived[name]:6.3f}"
            )
        elif value.get("event_type") == "finished":
            lines.append(f"{time:7.4f} {name}: Finished")
    return lines
