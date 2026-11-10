"""EX-1: Bank with customers who give up waiting. The plain SimPy program: from the SimPy program.

The three forms are the files of the example `simpy-bank-renege`.
"""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets
EXAMPLE = "bank_renege"  # the forms are the files of SimPy's Bank Renege example


def program():
    env, log, L = new()

    def source(number, interval, counter):
        for i in range(number):
            c = customer(env, f"Customer{i:02d}", counter, time_in_bank=12.0)
            env.process(c)
            t = 10.0
            yield env.timeout(t)

    def customer(env, name, counter, time_in_bank):
        arrive = env.now
        with counter.request() as req:
            patience = [3, 1.5, 3, 2, 3][
                int(name[-2:])
            ]  # renege, tie and wait for the unit
            results = yield (req | env.timeout(patience))
            env.now - arrive
            if req in results:
                tib = 12.0
                yield env.timeout(tib)
                L(name, "customer", {"event_type": "finished"})
            else:
                L(name, "customer", S("reneged"))

    counter = simpy.Resource(env, capacity=1)
    env.process(source(5, 10.0, counter))
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    ("dist: exponential\n    rate: 0.1", "dist: constant\n    value: 10"),
    (
        "patience: {gen: uniform, low: 1, high: 3}",
        'patience: {gen: expr, expr: "[3, 1.5, 3, 2, 3][n % 5]", names: {n: {gen: sequence}}}',
    ),
    (
        "service_time: {gen: sample, dist: exponential, rate: 0.08333333333333333}",
        "service_time: 12",
    ),
]
