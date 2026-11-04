"""DS-V4: A random value drawn for each entity and used as a priority. The plain SimPy program: from the design's SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/V4.py: four jobs created at 0 draw 3, 1, 2, 0 in creation order and request a one-unit priority resource with that value; each is served 1 s.
    env, log, L = new()
    m = simpy.PriorityResource(env, 1)
    draws = {
        "j0": 3,
        "j1": 1,
        "j2": 2,
        "j3": 0,
    }  # the source draws from an iterator in creation order; here each job holds the value it gets in the source order, so the tie-order variants keep them

    def job(k):
        x = draws[k.split("-")[0]]
        with m.request(priority=x) as r:
            yield r
            yield env.timeout(1)
            L(k, "job", {"event_type": "served", "x": x})

    for n in ("j0", "j1", "j2", "j3"):
        arrival(env, n, job, gap=1, max_events=1, first=0)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to
# model.yaml, one for each arrival in the order j0, j1, j2, j3.
_DRAWN = "attributes: {x: {gen: sample, dist: exponential, rate: 2}}"
DRAWS = [(_DRAWN, f"attributes: {{x: {x}}}") for x in (3, 1, 2, 0)]
