"""GD-V4: A process is an event and its value is returned by the run. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program(n=None, rev=False):
    # events.rst, "Processes are events, too": parent() yields the process sub() and returns its value; env.run(env.process(parent(env)))
    env, log, L = new()

    def sub(k):
        yield env.timeout(1)
        L(k, "sub", {"ret": 23})
        return 23

    def parent(k):
        ret = yield env.process(sub(k))
        L(k, "parent", {"ret": ret})
        return ret

    procs = [env.process(parent(f"a-{i + 1}")) for i in range(n or 1)]
    return go(env, log, procs[0])


program.ties = True
