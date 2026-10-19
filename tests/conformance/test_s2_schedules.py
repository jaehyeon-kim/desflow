"""S2 schedules: changes at set times, with clear rules at the boundaries.

A step at time t applies before every other event at t, as a plain SimPy process
whose timeout was scheduled first would, so the arrival gap drawn at t already uses
the new value. Steps at the same time apply in the order given. A step at exactly
`until` never applies, as no event at `until` runs.
"""

import simpy

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicResource,
    Sampler,
    SimParameter,
    run_scenario,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import run_builder, run_low_level, run_yaml

STEPS = [
    {"at": 10, "path": "S2.arrival.orders.value", "value": 20},
    {"at": 25, "path": "S2.resources.packers.current_cap", "value": 3},
    {"at": 25, "path": "S2.resources.packers.current_cap", "value": 2},
    {"at": 40, "path": "S2.resources.packers.current_cap", "value": 4},
]


def _reference():
    """Arrival times and the packers' capacity at 40, in plain SimPy: the schedule
    process is started first, so its timeouts come before the source's at equal
    times."""
    env = simpy.Environment()
    state = {"gap": 10, "packers": 1}
    arrivals = []

    def schedule():
        for step in STEPS:
            if step["at"] >= 40:
                return
            yield env.timeout(step["at"] - env.now)
            key = "gap" if step["path"].endswith("value") else "packers"
            state[key] = step["value"]

    def source():
        while True:
            yield env.timeout(state["gap"])
            arrivals.append(env.now)

    env.process(schedule())
    env.process(source())
    env.run(until=40)
    return arrivals, state["packers"]


def _arrivals(records):
    return [r["sim_ts"] for r in records if r["stream_type"] == "event"]


def _params():
    return SimParameter(
        sim_id="S2",
        arrival={"orders": DistributionConfig(dist="constant", value=10)},
        resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
    )


def test_s2_step_at_an_arrival_time():
    """The step at 10 falls on an arrival: the next gap is 20, so orders arrive at
    10 and 30. Equal-time steps at 25 leave 2 packers, and the step at 40 never
    applies."""
    expected = _reference()
    assert expected == ([10, 30], 2)

    built = {}

    def start(env):
        built["packers"] = DynamicResource(env, "S2", "packers")
        sampler = Sampler(seed=1)

        def source():
            n = 0
            while True:
                gap = sampler.sample(env.registry.get_config("S2.arrival.orders"))
                yield env.timeout(gap)
                n += 1
                env.publish_event(
                    f"o-{n}", {"event": "arrived"}, task="take", instance=n
                )

        env.process(run_scenario(env, STEPS, until=40))
        env.process(source())

    records, _env = run_low_level(_params(), start, 40)
    assert (_arrivals(records), built["packers"].capacity) == (
        [10.0, 30.0],
        expected[1],
    )

    app = SimulationContext("S2", factor=0.0)
    app.add_resource("packers", current_cap=1, max_cap=4)
    app.add_arrival("orders", dist="constant", value=10)
    app.add_scenario(STEPS)

    @app.task()
    def take(task_id):
        return {"event": "arrived"}

    @app.arrival_loop("orders")
    def orders(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(take(context.next_instance("take"), entity=entity))

    records = run_builder(app, 40)
    assert (_arrivals(records), app.get_resource("packers").capacity) == (
        [10.0, 30.0],
        2,
    )
    records, yaml_app = run_yaml("s2_schedule.yaml")
    assert (_arrivals(records), yaml_app.get_resource("packers").capacity) == (
        [10.0, 30.0],
        2,
    )
