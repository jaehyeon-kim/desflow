"""DS-V12: Put back the item a get took. The plain SimPy program: from the design's SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/V12.py part 1 (FilterStore with a mapping, a number and a zero; a second get; a put of the first item) and part 4 (an item sets a later time)
    env, log, L = new()
    st = simpy.FilterStore(env)
    st.items.extend([{"req": {"id": 7}}, {"req": {"id": 8}}, 5, 0])
    st2 = simpy.Store(env)
    st2.items.append({"d": 3})

    def p(k):
        it = yield st.get(lambda i: isinstance(i, dict) and i["req"]["id"] == 7)
        L(k, "take", {"event_type": "took", "item": it})
        second = yield st.get(lambda i: i == 5)
        L(k, "second", {"event_type": "second", "item": second})
        third = yield st.get(lambda i: i == 0)
        L(k, "third", {"event_type": "third", "item": third})
        yield st.put(it)
        L(k, "back", {"event_type": "back"})
        yield st.put(5)
        L(k, "num", {"event_type": "num"})
        L(
            k, "report", {"event_type": "items", "items": list(st.items)}
        )  # the source logs after the put has finished: a task after it

    def after_get(k):
        it = yield st2.get()
        yield env.timeout(it["d"])
        L(k, "after_get", {"event_type": "used", "at": env.now})

    arrival(env, "p", p, gap=3, max_events=1, first=0)
    arrival(env, "q", after_get, gap=3, max_events=1, first=0)
    return go(env, log)
