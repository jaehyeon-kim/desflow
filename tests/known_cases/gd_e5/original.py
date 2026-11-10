"""GD-E5: Read which process is running. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program(n=None, rev=False):
    # environments.rst, "State access": my_proc() prints env.active_process, calls subfunc() which prints it again, then yields a timeout. One env.step() runs it to the first yield.
    env, log, L = new()

    def subfunc(env, k):
        L(
            k, "sub", {"event_type": "seen_in_sub", "me": env.active_process.name}
        )  # print(env.active_process)

    def my_proc(env, k):
        while True:
            L(
                k, "my_proc", {"event_type": "seen", "me": env.active_process.name}
            )  # print(env.active_process)
            subfunc(env, k)
            yield env.timeout(1)

    procs = [env.process(my_proc(env, f"p1-{i + 1}")) for i in range(n or 1)]
    assert env.active_process is None
    for _ in procs:
        env.step()
    assert env.active_process is None
    return log, None


program.ties = True
