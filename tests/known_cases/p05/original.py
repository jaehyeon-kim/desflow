"""P05: A group of members arrives together at each arrival. The plain SimPy program: written for this case from the row, because the register's program was not kept."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import PROCS, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    made = [0]
    tick = [0]
    SIZES = [3, 0, 1, 2]  # a group of 0 starts nobody and takes no key number

    def member(k):
        L(k, "member", {"event_type": "member"})
        yield env.timeout(0)

    def source():
        while True:
            yield env.timeout(5)
            size = SIZES[
                tick[0] % len(SIZES)
            ]  # the group size the document draws from Poisson(2)
            tick[0] += 1
            for _ in range(size):
                made[0] += 1
                key = f"a-{made[0]}"
                PROCS[key] = env.process(member(key))

    env.process(source())
    return go(env, log, 50)


# The draw is fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        "group: {gen: sample, dist: poisson, mean: 2}",
        'group: {gen: expr, expr: "[3, 0, 1, 2][n % 4]", names: {n: {gen: sequence}}}',
    )
]
