"""ST-U05: A subscriber interrupted by a timeout with a value. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_subscribe_at_timeout_with_value (test_subscribe_at_timeout is the same with no value)
    env, log, L = new()
    import simpy.util

    def make(
        k,
    ):  # the blueprint's task that fires the signal for the timeout; the test has no such process
        L(k, "make", {"event_type": "made"})
        return
        yield

    def pem(k):
        val = "ohai"
        to = env.timeout(2, value=val)
        simpy.util.subscribe_at(to)
        try:
            yield env.timeout(10)
            L(k, "pem", {"event_type": "should_not_finish"})
        except simpy.Interrupt as interrupt:
            assert interrupt.cause == (to, val)
            assert env.now == 2
            L(
                k,
                "pem",
                S("interrupted", cause={"event": None, "value": interrupt.cause[1]}),
            )
            L(
                k,
                "told",
                {
                    "event_type": "interrupted",
                    "at": env.now,
                    "cause": interrupt.cause[1],
                },
            )

    arrival(env, "m", make, initial=1)
    arrival(env, "p", pem, initial=1)
    return go(env, log)
