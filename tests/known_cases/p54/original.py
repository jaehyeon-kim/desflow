"""P54: Assembly from two stores. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # patterns/f_flow.py F2: parts a every 1, b every 2, an assembler entity every 0.5, until 4.2
    env, log, L = new()
    sa = simpy.Store(env)
    sb = simpy.Store(env)

    def pa(k):
        yield sa.put(1)
        L(k, "pa", {"part": "a"})

    def pb(k):
        yield sb.put(1)
        L(k, "pb", {"part": "b"})

    def assemble(k):
        yield sa.get() & sb.get()
        L(k, "assemble", {"done": 1})

    arrival(env, "a", pa, gap=1)
    arrival(env, "b", pb, gap=2)
    arrival(env, "asm", assemble, gap=0.5)
    return go(env, log, 4.2)
