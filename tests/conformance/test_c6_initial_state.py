"""C6 initial state: resources, inventory and entities start as declared.

The model starts its clock at 50 with two packers, 6 boxes of stock and two
parcels already in the store. A process reads them at the initial time, before the
first arrival, and the first order at 55 carries the attributes its arrival
declares. A plain SimPy model built with the same values sets the expected state.
"""

import simpy

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DistributionConfig,
    DynamicContainer,
    DynamicResource,
    DynamicStore,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import run_builder, run_low_level, run_yaml
from tests.conformance.state import snapshot

ATTRIBUTES = {"size": 3, "gift": True}


def _reference():
    env = simpy.Environment(initial_time=50)
    packers = simpy.Resource(env, capacity=2)
    stock = simpy.Container(env, capacity=10, init=6)
    parcels = simpy.Store(env, capacity=5)
    parcels.items.extend(["p1", "p2"])
    state = {}

    def read():
        state["initial"] = (
            env.now,
            packers.capacity,
            stock.level,
            list(parcels.items),
        )
        yield env.timeout(0)

    def order(attributes):
        state["first order"] = (env.now, dict(attributes))
        yield env.timeout(0)

    def source():
        yield env.timeout(5)
        env.process(order(ATTRIBUTES))

    env.process(read())
    env.process(source())
    env.run(until=56)
    return state


def _state(records):
    """The same state, read from the published records."""
    telemetry = {
        r["path_id"].split(".")[-1]: (r["sim_ts"], r["value"])
        for r in records
        if r["stream_type"] == "telemetry" and ".initial." in r["path_id"]
    }
    first = next(
        r for r in records if r["stream_type"] == "event" and r["task"] == "order"
    )
    return {
        "initial": (
            telemetry["packers"][0],
            telemetry["packers"][1],
            telemetry["stock"][1],
            telemetry["parcels"][1],
        ),
        "first order": (
            first["sim_ts"],
            {name: first["value"][name] for name in ATTRIBUTES},
        ),
    }


class _LowLevelContext:
    """What `snapshot` reads, for a low-level model."""

    def __init__(self, env, shop):
        self.env, self._shop = env, shop

    def publish(self, name, value):
        self.env.publish_telemetry(f"C6.{name}", value)

    def get_resource(self, name):
        return self._shop[name]

    get_container = get_store = get_resource


def _low_level():
    params = SimParameter(
        sim_id="C6",
        arrival={"orders": DistributionConfig(dist="constant", value=5)},
        resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
        containers={"stock": ContainerConfig(current_cap=10, max_cap=10, level=6)},
        stores={"parcels": CapacityConfig(current_cap=5, max_cap=5)},
    )

    def start(env):
        shop = {
            "packers": DynamicResource(env, "C6", "packers"),
            "stock": DynamicContainer(env, "C6", "stock"),
            "parcels": DynamicStore(env, "C6", "parcels", items=["p1", "p2"]),
        }
        env.process(snapshot(_LowLevelContext(env, shop)))

        def source():
            yield env.timeout(5.0)
            env.publish_event("orders-1", dict(ATTRIBUTES), task="order", instance=0)

        env.process(source())

    return run_low_level(params, start, 56, initial_time=50)[0]


def _builder():
    app = SimulationContext("C6", factor=0.0, initial_time=50)
    app.add_resource("packers", current_cap=2, max_cap=4)
    app.add_container("stock", current_cap=10, max_cap=10, level=6)
    app.add_store("parcels", current_cap=5, max_cap=5, items=["p1", "p2"])
    app.add_arrival("orders", dist="constant", value=5, attributes=ATTRIBUTES)
    app.add_process(snapshot)

    @app.task(name="order")
    def order(task_id, entity):
        return dict(entity.attributes)

    @app.arrival_loop("orders")
    def orders(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(order(context.next_instance("order"), entity=entity))

    return run_builder(app, 56)


def _check(part):
    expected = _reference()[part]
    for records in (_low_level(), _builder(), run_yaml("initial_state.yaml")[0]):
        assert _state(records)[part] == expected
    return expected


def test_c6_initial_resources():
    """Two packers at the initial time, 50, before the first arrival."""
    time, packers, _stock, _parcels = _check("initial")
    assert (time, packers) == (50, 2)


def test_c6_initial_inventory():
    """6 boxes of stock and two parcels at the initial time, 50."""
    time, _packers, stock, parcels = _check("initial")
    assert (time, stock, parcels) == (50, 6, ["p1", "p2"])


def test_c6_initial_entity_state():
    """The first order, at 55, carries the attributes its arrival declares."""
    assert _check("first order") == (55, ATTRIBUTES)
