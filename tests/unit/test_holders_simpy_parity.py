"""A holder grants in the order its SimPy twin does, on random models.

For each of 20 fixed seeds, a model of requests at mixed times, amounts and priorities
runs on a `DynamicResource`, `DynamicStore` or `DynamicContainer` and on the SimPy
class with the same behaviour: `Resource`, `PriorityResource`, `PreemptiveResource`,
`Store` and `Container`. The model has no capacity change, no breakdown and no
`queue_limit`. The trace of who is served, preempted or finished, and when, must be
identical. The resource runs reuse the jobs and runners of the conformance tests.
"""

import random
from typing import Any, Callable, List, Tuple

import pytest
import simpy

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DynamicContainer,
    DynamicStore,
    SimParameter,
)
from tests.conformance import harness
from tests.conformance.b3 import Job, run_low_level, run_simpy

SEEDS = range(20)
UNTIL = 60

Trace = List[Tuple[float, str, str]]


def _jobs(rng: random.Random, priorities: bool, preempt: bool) -> List[Job]:
    """Eight to twelve jobs; request times fall on whole seconds so ties are common."""
    return [
        Job(
            f"j{n}",
            at=float(rng.randint(0, 20)),
            hold=float(rng.randint(1, 8)),
            priority=float(rng.randint(0, 3)) if priorities else None,
            preempt=rng.random() < 0.7 if preempt else True,
        )
        for n in range(rng.randint(8, 12))
    ]


RESOURCES = {
    "fifo": (lambda env, cap: simpy.Resource(env, cap), {}, False, False),
    "priority": (
        lambda env, cap: simpy.PriorityResource(env, cap),
        {"queue": "priority"},
        True,
        False,
    ),
    "preemptive": (
        lambda env, cap: simpy.PreemptiveResource(env, cap),
        {"queue": "priority", "preempt": True},
        True,
        True,
    ),
}


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("kind", RESOURCES)
def test_a_resource_serves_in_the_order_of_its_simpy_twin(kind, seed):
    make, options, priorities, preempt = RESOURCES[kind]
    rng = random.Random(f"{kind}-{seed}")
    capacity = rng.randint(1, 3)
    while True:
        jobs = _jobs(rng, priorities, preempt)
        try:
            expected = run_simpy(lambda env: make(env, capacity), jobs, UNTIL).trace
            break
        except ValueError:
            # The shared job process, not SimPy's resource, fails on some preemption
            # orders (a negative remaining time); draw another set of jobs
            continue
    spec = {"current_cap": capacity, "max_cap": capacity, **options}
    assert run_low_level(spec, jobs, UNTIL).trace == expected


def _store_program(env: Any, store: Any, rng: random.Random, trace: Trace) -> None:
    """Putters and getters that start at random whole seconds; each notes when its
    request is granted."""

    def put(name: str, at: float) -> Any:
        yield env.timeout(at)
        yield store.put(name)
        trace.append((env.now, name, "put"))

    def get(name: str, at: float) -> Any:
        yield env.timeout(at)
        item = yield store.get()
        trace.append((env.now, name, f"got {item}"))

    for n in range(rng.randint(8, 12)):
        at = float(rng.randint(0, 20))
        if rng.random() < 0.5:
            env.process(put(f"p{n}", at))
        else:
            env.process(get(f"g{n}", at))


def _container_program(env: Any, tank: Any, rng: random.Random, trace: Trace) -> None:
    """Puts and gets of mixed amounts, so a large request at the head of a queue
    holds back the smaller ones behind it."""

    def move(name: str, at: float, action: Callable, amount: int) -> Any:
        yield env.timeout(at)
        yield action(amount)
        trace.append((env.now, name, f"moved {amount}"))

    for n in range(rng.randint(8, 12)):
        at, amount = float(rng.randint(0, 20)), rng.randint(1, 5)
        if rng.random() < 0.5:
            env.process(move(f"p{n}", at, tank.put, amount))
        else:
            env.process(move(f"g{n}", at, tank.get, amount))


def _simpy_trace(make: Callable, program: Callable, seed: str) -> Trace:
    env = simpy.Environment()
    trace: Trace = []
    program(env, make(env), random.Random(seed), trace)
    env.run(until=UNTIL)
    return trace


def _desflow_trace(
    params: SimParameter, make: Callable, program: Callable, seed: str
) -> Trace:
    trace: Trace = []

    def start(env: Any) -> None:
        program(env, make(env), random.Random(seed), trace)

    harness.run_low_level(params, start, UNTIL)
    return trace


@pytest.mark.parametrize("seed", SEEDS)
def test_a_store_grants_puts_and_gets_in_the_order_of_simpy_store(seed):
    rng = random.Random(f"store-{seed}")
    capacity = rng.randint(1, 3)
    name = f"store-{seed}"
    expected = _simpy_trace(
        lambda env: simpy.Store(env, capacity), _store_program, name
    )
    params = SimParameter(
        sim_id="P",
        stores={"shelf": CapacityConfig(current_cap=capacity, max_cap=capacity)},
    )
    actual = _desflow_trace(
        params, lambda env: DynamicStore(env, "P", "shelf"), _store_program, name
    )
    assert actual == expected


@pytest.mark.parametrize("seed", SEEDS)
def test_a_container_grants_puts_and_gets_in_the_order_of_simpy_container(seed):
    rng = random.Random(f"container-{seed}")
    capacity, level = rng.randint(5, 10), rng.randint(0, 5)
    name = f"container-{seed}"
    expected = _simpy_trace(
        lambda env: simpy.Container(env, capacity, init=level), _container_program, name
    )
    params = SimParameter(
        sim_id="P",
        containers={
            "tank": ContainerConfig(current_cap=capacity, max_cap=capacity, level=level)
        },
    )
    actual = _desflow_trace(
        params, lambda env: DynamicContainer(env, "P", "tank"), _container_program, name
    )
    assert actual == expected
