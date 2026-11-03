"""ST-C10: Build flat conditions with in-place and. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_condition.py test_iand_with_and_cond, test_iand_with_or_cond, test_ior_with_or_cond, test_ior_with_and_cond (ST-C10 to C13), one after the other
    env, log, L = new()

    def stage(k, task, first, then):
        t1 = env.timeout(1, value=1)
        t2 = env.timeout(2, value=2)
        cond = t1 & t2 if first == "&" else t1 | t2
        orig = cond
        t0 = env.timeout(0, value=0)
        if then == "&":
            cond &= t0
        else:
            cond |= t0
        assert cond is not orig
        results = yield cond
        L(
            k,
            task,
            {
                "event_type": "done",
                "at": env.now,
                "values": list(results.values()),
                "in_result": [t in results for t in (t1, t2, t0)],
            },
        )

    def pem(k):
        yield from stage(k, "iand_and", "&", "&")
        yield from stage(k, "iand_or", "|", "&")
        yield from stage(k, "ior_or", "|", "|")
        yield from stage(k, "ior_and", "&", "|")

    arrival(env, "a", pem, initial=1)
    return go(env, log)
