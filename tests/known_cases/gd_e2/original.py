"""GD-E2: Run until a process ends and return its value. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program(n=None, rev=False):
    # environments.rst, "Simulation control": my_proc() waits 1 and returns a string; env.run(until=proc) returns it
    env, log, L = new()

    def my_proc(k):
        yield env.timeout(1)
        L(k, "my_proc", {"text": "Monty Python’s Flying Circus"})
        return "Monty Python’s Flying Circus"

    procs = [env.process(my_proc(f"a-{i + 1}")) for i in range(n or 1)]
    return go(env, log, procs[0])


program.ties = True
