"""GD-V7: Or, and, nested conditions and timeout values. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # events.rst, test_condition(): t1 | t2, then t1 & t2, then (e1 | e2) & e3; the asserts of the guide are kept
    env, log, L = new()

    def test_condition(k):
        t1, t2 = env.timeout(1, value="spam"), env.timeout(2, value="eggs")
        ret = yield t1 | t2
        assert ret == {t1: "spam"}
        L(
            k,
            "any_stage",
            {
                "event_type": "any_done",
                "first": ret[t1],
                "second_missing": t2 not in ret,
            },
        )

        t1, t2 = env.timeout(1, value="spam"), env.timeout(2, value="eggs")
        ret = yield t1 & t2
        assert ret == {t1: "spam", t2: "eggs"}
        L(k, "all_stage", {"event_type": "all_done", "values": [ret[t1], ret[t2]]})

        # You can also concatenate & and |
        e1, e2, e3 = [env.timeout(i) for i in range(3)]
        yield (e1 | e2) & e3
        assert all(e.processed for e in [e1, e2, e3])
        L(
            k,
            "nest_stage",
            {
                "event_type": "nest_done",
                "all_processed": all(e.processed for e in [e1, e2, e3]),
            },
        )

    arrival(env, "a", test_condition, initial=1)
    return go(env, log)
