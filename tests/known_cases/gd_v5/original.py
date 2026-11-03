"""GD-V5: A process started after a delay, then waited for. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program(n=None, rev=False):
    # events.rst, start_delayed: parent() starts sub() after a delay of 3, yields the helper process, then yields sub_proc; env.run(env.process(parent(env)))
    from simpy.util import start_delayed

    env, log, L = new()

    def sub(k):
        L(k, "delayed_sub", {"event_type": "starting"})
        yield env.timeout(1)
        L(k, "sub", {"ret": 23})
        return 23

    def parent(k):
        sub_proc = yield start_delayed(env, sub(k), delay=3)
        ret = yield sub_proc
        L(k, "parent", {"event_type": "parent_done", "ret": ret})
        return ret

    procs = [env.process(parent(f"a-{i + 1}")) for i in range(n or 1)]
    return go(env, log, procs[0])


program.ties = True
