"""DS-V7: Names for a cause, holders, an item priority and nested values. The plain SimPy program: from the design's SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/V7.py: (1) a low-priority (5) holder of a preemptive unit for 10 is preempted by a priority-1 request at the same instant, with the cause's by, usage_since and the time left, and the holders at each point; (2) the holders of a unit of two in grant order; (3) a priority store with items put at priority 2 and 1 and two gets.
    env, log, L = new()
    m = simpy.PreemptiveResource(env, 1)
    m2 = simpy.Resource(env, 2)
    st = simpy.PriorityStore(env)
    names = {}

    def low(k):
        names[env.active_process] = k
        with m.request(priority=5) as r:
            yield r
            began = env.now  # the source logs the holders here; a `resource` task has no record at the grant
            try:
                yield env.timeout(10)
            except simpy.Interrupt as i:
                c = i.cause
                L(
                    k,
                    "low",
                    S(
                        "preempted",
                        resource="m",
                        preempted_by=names[c.by],
                        usage_since=c.usage_since,
                    ),
                )
                L(
                    k,
                    "low_cause",
                    {
                        "event_type": "cause",
                        "by": names[c.by],
                        "usage_since": c.usage_since,
                        "left": 10 - (env.now - began),
                        "state": "interrupted",
                        "now": env.now,
                    },
                )

    def high(k):
        names[env.active_process] = k
        yield env.timeout(0)
        with m.request(priority=1) as r:
            yield r
            L(
                k,
                "high_seize",
                {"event_type": "high_granted", "holders": len(m.users), "at": env.now},
            )

    def hold2(k):
        names[env.active_process] = k
        with m2.request() as r:
            yield r
            L(
                k,
                "hold2",
                {"event_type": "users", "who": [names[u.proc] for u in m2.users]},
            )
            yield env.timeout(5)

    def put1(k):
        yield st.put(simpy.PriorityItem(2, {"req": {"id": 7}}))
        L(k, "put1", {"req": {"id": 7}})
        yield st.put(simpy.PriorityItem(1, {"req": {"id": 8}}))
        L(k, "put2", {"req": {"id": 8}})
        for t in ("get1", "get2"):
            it = yield st.get()
            L(
                k,
                t,
                {"event_type": "got", "prio": it.priority, "id": it.item["req"]["id"]},
            )

    def end(k):
        L(k, "end", {"event_type": "holders_at_the_end", "holders": len(m.users)})
        yield env.timeout(0)

    for n, f, at in (
        ("low", low, 0),
        ("high", high, 0),
        ("a", hold2, 0),
        ("b", hold2, 0),
        ("pr", put1, 0),
        ("fin", end, 20),
    ):
        arrival(env, n, f, gap=1, max_events=1, first=at)
    return go(env, log)
