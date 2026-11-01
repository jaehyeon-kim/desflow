"""The SimPy programs of CV-040, written from `cv_040_segments.yaml` and
`cv_040_every.yaml` (independence class: written from the blueprint). The program
runs in segments, reads the model between them, and starts a process that reads it
at the stop, as the SimPy documentation's loop of `env.run(until=...)` does."""

import math
from typing import Any, List, Tuple

import simpy

Trace = List[Tuple[Any, ...]]


def _reading(trace: Trace, env: simpy.Environment, index: int) -> None:
    head = env.peek()
    trace.append(("segment", index, env.now, None if math.isinf(head) else head))


def with_look(n: int) -> Trace:
    """Segments to 2 with a look, to the next firing of a signal, to 4, then to 6."""
    env = simpy.Environment()
    trace: Trace = []
    gate = {"event": env.event()}

    def tick(index: int):
        trace.append((env.now, f"a-{index}", "started"))
        yield env.timeout(1)
        trace.append((env.now, f"a-{index}", "done"))
        fired, gate["event"] = gate["event"], env.event()
        fired.succeed()

    def source():
        for index in range(1, n + 1):
            yield env.timeout(1 - env.now)
            env.process(tick(index))
        yield env.event()

    def looking():
        trace.append((env.now, "looking-1", "done"))
        return
        yield  # a generator, which ends at its first step

    env.process(source())
    env.run(until=2)
    look = env.process(looking())
    while not look.triggered:
        env.step()
    _reading(trace, env, 0)
    env.run(until=gate["event"])
    _reading(trace, env, 1)
    env.run(until=4)
    _reading(trace, env, 2)
    env.run(until=6)
    return trace


def every(n: int) -> Trace:
    """Segments at 2, 4 and 6, the end."""
    env = simpy.Environment()
    trace: Trace = []

    def tick(index: int):
        trace.append((env.now, f"a-{index}", "started"))
        yield env.timeout(1)
        trace.append((env.now, f"a-{index}", "done"))

    def source():
        for index in range(1, n + 1):
            yield env.timeout(1 - env.now)
            env.process(tick(index))
        yield env.event()

    env.process(source())
    for index, until in enumerate((2, 4, 6)):
        env.run(until=until)
        _reading(trace, env, index)
    return trace
