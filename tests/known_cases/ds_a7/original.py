"""DS-A7: A spawn count and copied attributes. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def _ckey(k, n):
    return f"clone-{n}"


def program():
    # research/yaml-only/programs/A7.py run(n, attrs): the parent starts n children, each with a copy of the attributes, and goes on first
    env, log, L = new()
    attrs = {"n": 3, "tag": "x"}
    cnt = [0]

    def child(k, a):
        L(k, "clone", {"event_type": "clone", "tag": a["tag"]})
        yield env.timeout(0)

    def parent(k):
        for i in range(attrs["n"]):
            cnt[0] += 1
            env.process(child(_ckey(k, cnt[0]), dict(attrs)))
        L(k, "parent", {"event_type": "parent"})
        yield env.timeout(1)

    arrival(env, "a", parent, gap=3, max_events=2, first=0)
    return go(env, log, 50)
