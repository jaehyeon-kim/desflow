"""DS-V2: A wait timeout computed for each entity. The plain SimPy program: from the design's SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/V2.py run(5, 1) and run(1, 5): a waiter on (ev | timeout(limit)) and a sender that fires ev at `at`; the two runs are two independent pairs in one environment.
    env, log, L = new()

    def pair(tag, limit, at):
        ev = env.event()

        def waiter(k):
            lim = env.timeout(limit)
            res = yield (ev | lim)
            L(
                k,
                "waiter" + tag,
                {"event_type": "left", "ev": ev in res, "limit": lim in res},
            )

        def sender(k):
            yield env.timeout(at)
            ev.succeed(1)

        return waiter, sender

    w1, s1 = pair("", 5, 1)
    w2, s2 = pair("2", 1, 5)
    arrival(env, "w1", w1, gap=1, max_events=1, first=0)
    arrival(env, "s1", s1, gap=1, max_events=1, first=0)
    arrival(env, "w2", w2, gap=1, max_events=1, first=0)
    arrival(env, "s2", s2, gap=1, max_events=1, first=0)
    return go(env, log)


# The draws are fixed, as in fixed_draws.py: the test applies these replacements to model.yaml.
DRAWS = [
    ("limit: {gen: integer, bounds: [3, 7]}", "limit: 5"),
    ("limit: {gen: integer, bounds: [1, 3]}", "limit: 1"),
]
