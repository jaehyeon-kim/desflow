"""GD-RS9: A filter store of machines. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # resources.rst, FilterStore of machines: Machine(size, duration), user(name, env, ms, size) for i in range(3), size (i % 2) + 1
    from collections import namedtuple

    env, log, L = new()
    Machine = namedtuple("Machine", "size, duration")
    m1 = Machine(1, 2)  # Small and slow
    m2 = Machine(2, 1)  # Big and fast
    machine_shop = simpy.FilterStore(env, capacity=2)
    machine_shop.items = [m1, m2]  # Pre-populate the machine shop

    def user(k, env, ms, size):
        machine = yield ms.get(lambda machine: machine.size == size)
        L(
            k,
            "take",
            {"event_type": "took", "size": machine.size, "duration": machine.duration},
        )
        yield env.timeout(machine.duration)
        L(k, "use", {"event_type": "used"})
        yield ms.put(machine)
        L(k, "back", {"event_type": "released"})

    for i in range(3):
        arrival(
            env,
            f"u{i}",
            lambda k, i=i: user(k, env, machine_shop, (i % 2) + 1),
            initial=1,
        )
    return go(env, log)
