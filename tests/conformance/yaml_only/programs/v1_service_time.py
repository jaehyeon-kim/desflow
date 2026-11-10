"""The SimPy program of V1: a service time computed for each car from its fuel, on one
pump. A car that is interrupted while it is served keeps the time it has left
(`resume`) or starts its service again (`restart`)."""

from typing import Any, List, Optional, Tuple

import simpy

Trace = List[Tuple[Any, ...]]


def reference(
    fuels: List[float], cut_at: Optional[float] = None, policy: str = "resume"
) -> Trace:
    """The (time, key, status) of the events of one car for each fuel. The cars ask
    for the pump at time 0 in list order; the first car with 6 units of fuel is
    interrupted at `cut_at`."""
    env = simpy.Environment()
    pump = simpy.Resource(env, 1)
    trace: Trace = []

    def car(index: int, fuel: float):
        key = f"cars-{index}"
        trace.append((env.now, key, "queued"))
        with pump.request() as request:
            yield request
            trace.append((env.now, key, "started"))
            full = fuel / 2
            left = full
            while True:
                began = env.now
                try:
                    yield env.timeout(left)
                    break
                except simpy.Interrupt:
                    left = left - (env.now - began) if policy == "resume" else full
                    trace.append((env.now, key, "interrupted"))
                    trace.append((env.now, key, "resumed"))
        trace.append((env.now, key, "done"))

    cars = [env.process(car(i, fuel)) for i, fuel in enumerate(fuels, start=1)]
    if cut_at is not None:
        target = next(c for c, fuel in zip(cars, fuels) if fuel == 6)

        def cutter():
            yield env.timeout(cut_at)
            target.interrupt()

        env.process(cutter())
    env.run()
    return trace
