"""ST-U02: A subscriber interrupted when another process ends. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_subscribe: the child waits 3 and returns 'ohai'; the parent subscribes and waits for an event that never comes
    env, log, L = new()
    import simpy.util

    def child(env, k):
        yield env.timeout(3)
        L(k, "other", {"event_type": "other_done", "value": "ohai"})
        return "ohai"

    def parent(k):
        child_proc = env.process(child(env, k))
        simpy.util.subscribe_at(child_proc)
        try:
            yield env.event()
        except simpy.Interrupt as interrupt:
            assert interrupt.cause is not None
            assert interrupt.cause[0] is child_proc
            assert interrupt.cause[1] == "ohai"
            assert env.now == 3
            # the cause of simpy.util.subscribe_at is (process, value); the record names the task instead of the process
            # desflow's cause value is the child's last payload (coverage.md:78), so the record shows the payload with the field value; the told record carries the returned value
            L(
                k,
                "subscriber",
                S(
                    "interrupted",
                    cause={
                        "event": "other",
                        "value": {
                            "event_type": "other_done",
                            "value": interrupt.cause[1],
                        },
                    },
                ),
            )
            L(
                k,
                "told",
                {
                    "event_type": "interrupted",
                    "at": env.now,
                    "value": interrupt.cause[1],
                },
            )

    arrival(env, "s", parent, initial=1)
    return go(env, log)
