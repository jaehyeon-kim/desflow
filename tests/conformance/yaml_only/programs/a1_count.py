"""The SimPy program of A1: sources that start a fixed number of processes, two
seconds apart, the first after `first_at` when it is given. A run can stop its sources
at `stop_at`, which interrupts them; the processes already started finish."""

from typing import Any, List, Optional, Tuple

import simpy

Trace = List[Tuple[Any, ...]]


def reference(
    names: List[str],
    count: int,
    first_at: Optional[float] = None,
    stop_at: Optional[float] = None,
    service: float = 0,
) -> Trace:
    """The (time, key) of every process the sources start, in the order they end. A
    source with a `first_at` always yields a timeout first, also for 0. A task takes
    `service` seconds and is traced when it ends."""
    env = simpy.Environment()
    trace: Trace = []
    sources: List[simpy.Process] = []

    def task(name: str, index: int):
        if service:
            yield env.timeout(service)
        trace.append((env.now, f"{name}-{index}"))
        yield env.timeout(0)

    def source(name: str):
        started = 0
        try:
            if first_at is not None and count > 0:
                yield env.timeout(first_at)
                env.process(task(name, started + 1))
                started += 1
            for _ in range(started, count):
                yield env.timeout(2)
                env.process(task(name, started + 1))
                started += 1
        except simpy.Interrupt:
            return

    def stopper():
        yield env.timeout(stop_at)
        for process in sources:
            if process.is_alive:
                process.interrupt()

    for name in names:
        sources.append(env.process(source(name)))
    if stop_at is not None:
        env.process(stopper())
    env.run()
    return trace
