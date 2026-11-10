"""ST-R11: Read the users and queue of a resource. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_get_users: run(until=1) and run(until=2), reading users and queue after each
    env, log, L = new()
    res = simpy.Resource(env, 1)
    who = {}

    def process(k):
        who[env.active_process] = k
        with res.request() as req:
            yield req
            yield env.timeout(1)
            L(k, "pem", {"event_type": "used"})

    arrival(env, "users", process, initial=3)

    def look(k):
        L(
            k,
            "look",
            {
                "event_type": "look",
                "users": [who[e.proc] for e in res.users],
                "queue": [who[e.proc] for e in res.queue],
            },
        )

    log, err = go(env, log, 1)
    procs = list(PROCS.values())
    look("look-1")
    assert [e.proc for e in res.users] == procs[0:1] and [
        e.proc for e in res.queue
    ] == procs[1:]
    log, err = go(env, log, 2)
    look("look-2")
    assert [e.proc for e in res.users] == procs[1:2] and [
        e.proc for e in res.queue
    ] == procs[2:]
    return log, err
