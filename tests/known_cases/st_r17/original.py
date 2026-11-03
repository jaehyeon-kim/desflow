"""ST-R17: Read the queues of a container of capacity 1. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import MODE, PROCS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_container_get_queued: run(until=1) and run(until=2), reading both queues after each
    env, log, L = new()
    container = simpy.Container(env, 1)
    who = {}

    def proc(k, wait, what, task):
        who[env.active_process] = k
        yield env.timeout(wait)
        with getattr(container, what)(1) as req:
            yield req
            L(k, task, {"event_type": "got" if what == "get" else "put"})

    arrival(env, "g", lambda k: proc(k, 0, "get", "get_it"), initial=1)
    arrival(env, "p", lambda k: proc(k, 1, "put", "put_it"), initial=3)

    def look(k):
        L(
            k,
            "look",
            {
                "event_type": "look",
                "put_queue": [who[e.proc] for e in container.put_queue],
                "get_queue": [who[e.proc] for e in container.get_queue],
            },
        )

    base = not MODE["rev"] and MODE["n"] is None
    log, err = go(env, log, 1)
    list(PROCS.values())
    p0 = PROCS["g-1"]
    look("look-1")
    if base:
        assert [ev.proc for ev in container.put_queue] == [] and [
            ev.proc for ev in container.get_queue
        ] == [p0]
    log, err = go(env, log, 2)
    look("look-2")
    if base:
        assert [ev.proc for ev in container.put_queue] == [PROCS["p-3"]] and [
            ev.proc for ev in container.get_queue
        ] == []
    return log, err
