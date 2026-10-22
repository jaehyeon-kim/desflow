"""The regression scenarios, as `EXAMPLE_CASES` lists the examples.

Each scenario is written in YAML, the builder and the low-level API under
`scenarios/`, and `scripts/regression.py capture` runs the three through the twin-test
runners and fails when they give different records. A case is the scenario's name, its
YAML file, the extra its connectors import (kept for the shape of `EXAMPLE_CASES`;
the capture runs with every extra installed), the simulated seconds to compare, and the
processes its low-level version starts. `SCENARIO_RUN` holds how the builder and YAML
versions of a scenario that ends by its own rules are run.

This file uses nothing but its arguments, so it runs against the desflow of either side
of a comparison.
"""


def _ending(m, env, sampler):
    """A scenario that publishes the basic statistics every 30 seconds and ends by its
    own rules: its low-level `run_to_end` runs it."""
    env.setup_stats("Shop", "basic", interval=30.0)
    env.example_shop = m.build(env)
    return m.processes(env, sampler, env.example_shop)


def _all(m, env, sampler):
    """The week-long scenario: every statistic, published every hour."""
    env.setup_stats("Shop", "all", interval=3600.0)
    return m.processes(env, sampler, m.build(env))


def _basic(interval):
    """A scenario that publishes the basic statistics every `interval` seconds."""

    def processes(m, env, sampler):
        env.setup_stats("Shop", "basic", interval=interval)
        return m.processes(env, sampler, m.build(env, sampler))

    return processes


SCENARIO_CASES = [
    ("order_fulfilment", "order_fulfilment.yaml", None, 600, _basic(50.0)),
    ("breakdowns_and_shifts", "breakdowns_and_shifts.yaml", None, 600, _basic(70.0)),
    ("task_flow", "task_flow.yaml", None, 600, _basic(60.0)),
    ("long_run", "long_run.yaml", None, 604800, _all),
    ("live_parameters", "live_parameters.yaml", None, 900, _ending),
]

SCENARIO_RUN = {
    "live_parameters": {
        "drain": {"limit": 120},
        "stop": {"condition": "level.boxes == 0"},
    },
}
