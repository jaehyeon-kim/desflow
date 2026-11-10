"""The SimPy program of V4: each process draws a value when it starts and asks for a
priority resource by it. The draws come from the generator's own sequence, which the
blueprint's seed and the place of `x` in the file name: the documented rule
`SeedSequence(seed, spawn_key=(crc32(place),))`."""

import zlib
from typing import Any, List, Tuple

import numpy as np
import simpy

SEED = 42
PLACE = "arrivals.a.attributes.x"
Trace = List[Tuple[Any, ...]]


def reference(n: int) -> Trace:
    """The (time, key, status) of the queued, started and done events of `n` jobs that
    start at one instant."""
    rng = np.random.default_rng(
        np.random.SeedSequence(SEED, spawn_key=(zlib.crc32(PLACE.encode()),))
    )
    env = simpy.Environment()
    machine = simpy.PriorityResource(env, capacity=1)
    trace: Trace = []

    def job(index: int):
        key = f"a-{index}"
        x = float(rng.exponential(1 / 2))
        trace.append((env.now, key, "queued"))
        with machine.request(priority=x) as request:
            yield request
            trace.append((env.now, key, "started"))
            yield env.timeout(1)
        trace.append((env.now, key, "done"))

    for index in range(1, n + 1):
        env.process(job(index))
    env.run(until=30)
    return trace
