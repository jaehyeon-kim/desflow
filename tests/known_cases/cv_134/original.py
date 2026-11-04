"""CV-134: A computed list of events made now and waited for later. The plain SimPy program: from the program of the conformance test that held the row."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # the Batch 11 program open_requests(1) of the conformance test that held the row: each entity makes a request on each of three resources at 0.5 and keeps them in a list, waits for the last, cancels the list in order (a granted request stays), reads which are processed and the queues, and releases the last; the resource j is held until 3 - j
    env, log, L = new()
    rs = [simpy.Resource(env, capacity=1) for _ in range(3)]

    def job(k):
        yield env.timeout(0.5)
        L(k, "begin", {"e": "half"})
        qs = [r.request() for r in rs]
        L(k, "opener", {"e": "opened"})
        yield qs[2]
        L(k, "waiter", {"e": "got"})
        for q in qs:
            if not q.triggered:
                q.cancel()
        x = [[q.processed for q in qs], [len(r.queue) for r in rs]]
        L(k, "closer", {"e": "done", "x": x})
        rs[2].release(qs[2])

    def hold(k):
        j = int(k.rsplit("-", 1)[1]) - 1
        with rs[j].request() as q:
            yield q
            yield env.timeout(3 - j)
        L(k, "hold", {"e": "held"})

    arrival(env, "a", job, initial=1)
    arrival(env, "h", hold, initial=3)
    log, err = go(env, log)
    assert (
        [r[3]["x"] for r in log if r[2] == "closer"]
        == [[[False, False, True], [0, 0, 0]]]
    )  # stated (the Batch 11 program open_requests(1)): the two requests not granted are cancelled
    return log, err
