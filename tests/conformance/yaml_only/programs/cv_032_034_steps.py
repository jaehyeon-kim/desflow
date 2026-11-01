"""The SimPy program of CV-032 and CV-034, written from `cv_032_034_steps.yaml`
(independence class: written from the blueprint). A source starts n ticks at 1, in one
step, as the rows of one time start; each
publishes when it starts, waits 1, and publishes when it ends. A driver loop reads
`env.peek()` and calls `env.step()` until `until`, as the SimPy documentation does."""

import math
from typing import Any, Dict, List, Tuple

import simpy

UNTIL = 5


def reference(n: int) -> Tuple[List[Dict[str, Any]], List[Tuple[Any, str, str]]]:
    """The readings `{step, now, peek}` after each step, and the published events."""
    env = simpy.Environment()
    published: List[Tuple[Any, str, str]] = []

    def tick(index: int):
        published.append((env.now, f"a-{index}", "started"))
        yield env.timeout(1)
        published.append((env.now, f"a-{index}", "done"))

    def source():
        yield env.timeout(1)
        for index in range(1, n + 1):
            env.process(tick(index))
        yield env.event()

    env.process(source())
    readings: List[Dict[str, Any]] = []
    while env.peek() < UNTIL:
        env.step()
        head = env.peek()
        readings.append(
            {
                "step": len(readings) + 1,
                "now": env.now,
                "peek": None if math.isinf(head) else head,
            }
        )
    return readings, published
