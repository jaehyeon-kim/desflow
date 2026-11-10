"""DS-V3: Computed values in a write, a cause and a timeout value. The plain SimPy program: from the design's SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/V3.py main part and returned(): state writes (n + 1, total - 3) and a store built from n before the run, a worker (timeout 9) interrupted at 1 by a cutter with a cause read from the state that then changes, two timeouts whose value is fixed when each is created, and a parent that takes a returned number and the same number through a field.
    env, log, L = new()
    state = {"n": 1, "reason": "r1", "total": 10}
    built = simpy.Store(env)

    def writes(k):
        state["n"] += 1
        state["total"] -= 3
        built.items.extend(state["n"] * i for i in range(3))
        L(
            k,
            "writes_report",
            {
                "event_type": "writes",
                "n": state["n"],
                "total": state["total"],
                "items": list(built.items),
            },
        )
        yield env.timeout(0)

    def worker(k):
        try:
            yield env.timeout(9)
        except simpy.Interrupt as i:
            L(k, "worker", S("interrupted", cause=i.cause))
            L(k, "worker_cause", {"event_type": "cause", "cause": i.cause})

    def cutter(k):
        yield env.timeout(1)
        cause = state["reason"]
        state["reason"] = "changed in the same step"
        PROCS["wk-1"].interrupt(cause)
        L(k, "cutter", {"event_type": "cutter_goes_on_first"})

    def timed(k):
        made = env.now
        v = yield env.timeout(2, value=env.now * 2)
        L(
            k,
            "timed1",
            {"event_type": "value_fixed", "v": v, "made": made, "now": env.now},
        )
        made = env.now
        v = yield env.timeout(2, value=env.now * 2)
        L(
            k,
            "timed2",
            {"event_type": "value_fixed", "v": v, "made": made, "now": env.now},
        )

    def child(k, wrap):
        yield env.timeout(1)
        L(k, "child", {"v": 10})
        return {"v": 10} if wrap else 10

    def parent(k):
        plain = yield env.process(child(k, False))
        field = yield env.process(child(k, True))
        L(
            k,
            "parent2",
            {
                "event_type": "returned",
                "plain": plain,
                "field": field["v"],
                "same": plain == field["v"],
            },
        )

    for n, f in (
        ("wr", writes),
        ("wk", worker),
        ("ct", cutter),
        ("tm", timed),
        ("rt", parent),
    ):
        arrival(env, n, f, gap=1, max_events=1, first=0)
    return go(env, log)
