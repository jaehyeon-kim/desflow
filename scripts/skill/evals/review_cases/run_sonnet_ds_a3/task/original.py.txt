"""DS-A3: A group size drawn for each arrival. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/A3.py run([2, 0, 1]): the gap of 5 comes first, then the size is drawn from the list, then the members start.
    env, log, L = new()
    draws = iter([2, 0, 1])
    n = [0]

    def member(k):
        L(k, "member", {"event_type": "member"})
        yield env.timeout(0)

    def src():
        for a in range(3):
            yield env.timeout(5)
            for i in range(next(draws)):
                n[0] += 1
                env.process(member(f"a-{n[0]}"))

    env.process(src())
    return go(env, log, 50)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    (
        "group: {gen: sample, dist: poisson, mean: 2}",
        'group: {gen: expr, expr: "[2, 0, 1][int(t // 5) - 1]"}',
    )
]
