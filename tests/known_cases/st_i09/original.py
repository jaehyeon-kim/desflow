"""ST-I09: Two interrupts at once while waiting on timeouts of 0. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_interrupts.py test_concurrent_behaviour: proc_a is created first, proc_b interrupts it twice
    env, log, L = new()

    def proc_a(k):
        timeouts = [env.timeout(0) for i in range(2)]
        names = ["first_wait", "second_wait"]
        while timeouts:
            task = names[2 - len(timeouts)]
            try:
                yield timeouts.pop(0)
                L(k, task, {"event_type": "finished"})
            except simpy.Interrupt as i:
                L(k, task, S("interrupted", cause=i.cause))
        L(k, "end", {"event_type": "end"})

    def proc_b(k):
        for n in range(2):
            for q, a in list(
                PROCS.items()
            ):  # every proc_a that exists and is alive, as the task interrupt does; a creation order that starts proc_b first has none yet
                if q.startswith("a-") and a.is_alive:
                    a.interrupt()
            L(
                k,
                ["poke_twice", "poke_again"][n],
                {"event_type": ["poke", "poke_again"][n]},
            )
        return
        yield

    arrival(env, "a", proc_a, initial=1)
    arrival(env, "b", proc_b, initial=1)
    return go(env, log)
