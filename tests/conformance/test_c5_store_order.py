"""C5 equal-time puts and gets: a store without priority grants gets as simpy.Store.

Two orders wait for parcels. At 5, a courier puts two parcels and a third order asks
for one, all in the same step. SimPy's `Store` grants one waiting get each time it
is triggered, so the third order's request gives the first parcel to the first
order, and the second order is served only when the first put is processed.
desflow's store is a `FilterStore` underneath, so that a get can take a filter;
while no waiting get has a filter it must grant as `Store` does. The test reads,
in that step, which gets are triggered, and the time and item of every get.
"""

import simpy

from desflow import CapacityConfig, DynamicStore, SimParameter
from tests.conformance.harness import run_low_level


def _model(env, store, log):
    def order(n, at):
        yield env.timeout(at)
        item = yield store.get()
        log.append((env.now, f"order-{n}", item))

    def courier():
        yield env.timeout(5)
        store.put("parcel-1")
        store.put("parcel-2")
        third = store.get()
        # Which orders' gets are granted in this same step
        log.append(("granted", first_two[0].triggered, first_two[1].triggered))
        item = yield third
        log.append((env.now, "order-3", item))

    first_two = []

    def first(n):
        yield env.timeout(1)
        request = store.get()
        first_two.append(request)
        item = yield request
        log.append((env.now, f"order-{n}", item))

    env.process(first(1))
    env.process(first(2))
    env.process(courier())

    def late():
        yield env.timeout(6)
        store.put("parcel-3")

    env.process(late())


def _reference():
    env = simpy.Environment()
    log: list = []
    _model(env, simpy.Store(env, capacity=3), log)
    env.run(until=10)
    return log


def _low_level():
    log: list = []

    def start(env):
        _model(env, DynamicStore(env, "C5", "bin"), log)

    params = SimParameter(
        sim_id="C5", stores={"bin": CapacityConfig(current_cap=3, max_cap=3)}
    )
    run_low_level(params, start, 10)
    return log


def test_c5_equal_time_puts_and_gets_follow_simpy_store():
    """In the step at 5 only the first order's get is granted, as simpy.Store
    grants it; the second is served when the first put is processed, and the third
    order waits for the parcel of 6."""
    expected = _reference()
    assert expected[0] == ("granted", True, False)
    assert _low_level() == expected
