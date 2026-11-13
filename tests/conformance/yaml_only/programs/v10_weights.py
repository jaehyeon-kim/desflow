"""The SimPy program of V10: each entity waits until its arrival time, spends a timed
step in the first task, and is then sent to the `fast` or the `slow` task by one draw
whose shares are its two weights divided by their sum.

The program is a plain SimPy model: entities are processes, the routing step is a
timeout, and the next step is chosen when the timeout ends. The draw comes from the
stream a blueprint with `random_seed: 42` gives the router of the task `route`
(`default_rng(SeedSequence(42, spawn_key=stream_key("route.route")))`), so the two
choose the same tasks.

The program does not refuse a weight. A weight below 0, or weights that add up to 0,
are refused by desflow's design (every weight is 0 or more and the sum is above 0, see
the research note in `planning/desflow/yaml-only.md`): Python's `random.choices` accepts
a pair such as (2, -1), so the refusal is a stricter rule and not SimPy's behaviour.
The test of the refusals runs desflow only.
"""

from typing import List, Sequence, Tuple

import numpy as np
import simpy

from desflow.core.sampler import stream_key

SEED = 42
PLACE = "route.route"
STEP = 2  # the seconds of the first task, as `service_time` in the blueprint


def reference(
    weights: Sequence[Tuple[float, float]], arrivals: Sequence[float]
) -> List[Tuple[float, str]]:
    """The (time, task) each entity is routed to, in the order they are routed.

    Args:
        weights: The (fast, slow) pair of each entity, in the order the entities are
            created.
        arrivals: The time each entity starts the first task.
    """
    env = simpy.Environment()
    rng = np.random.default_rng(
        np.random.SeedSequence(SEED, spawn_key=stream_key(PLACE))
    )
    routed: List[Tuple[float, str]] = []

    def entity(arrive: float, fast: float, slow: float):
        yield env.timeout(arrive)
        yield env.timeout(STEP)
        chosen = "fast" if rng.random() * (fast + slow) < fast else "slow"
        routed.append((env.now, chosen))

    for (fast, slow), arrive in zip(weights, arrivals):
        env.process(entity(arrive, fast, slow))
    env.run()
    return routed
