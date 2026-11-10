"""CV-126: A computed number of members made during the run. The plain SimPy program: from the program of the conformance test that held the row."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # the Batch 11 program per_entity(3) of the conformance test that held the row: the entity i uses the key i % 2 and asks for a capacity of i + 1; the first entity of a key makes the member (setdefault), a second user of the key finds the first one's capacity, and a read of the key 7 gives 0 and makes nothing
    env, log, L = new()
    desks: dict = {}

    def job(k):
        i = int(k.rsplit("-", 1)[1]) - 1
        desk = desks.setdefault(i % 2, simpy.Resource(env, capacity=i + 1))
        with desk.request() as request:
            yield request
            yield env.timeout(2)
            other = desks.get(7)
            x = [
                len(desk.users),
                len(desk.queue),
                0 if other is None else len(other.users),
                sorted(desks),
            ]
            L(k, "job", {"e": "done", "x": x})

    arrival(env, "a", job, initial=3)
    log, err = go(env, log)
    assert (
        [(r[0], r[1], r[3]["x"]) for r in log]
        == [
            (2, "a-1", [1, 1, 0, [0, 1]]),
            (2, "a-2", [1, 0, 0, [0, 1]]),
            (4, "a-3", [1, 0, 0, [0, 1]]),
        ]
    )  # stated: a-3 has the key of a-1, so it finds the member of one unit that a-1 made and waits in its queue until 2
    return log, err
