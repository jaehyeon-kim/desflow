"""The SimPy program of A4: sources whose gap is computed from the time when each gap
starts. A run can stop its sources at `stop_at`, which interrupts them in the middle of
a gap; the tasks already started finish."""

from typing import Any, Callable, List, Optional, Tuple

import simpy

Trace = List[Tuple[Any, ...]]


def reference(
    names: List[str],
    gap: Callable[[float, int], float],
    until: float = 8,
    stop_at: Optional[float] = None,
    service: float = 0,
) -> Tuple[Trace, str]:
    """The (time, key) of every arrival, in the order the tasks end, and the error a
    negative gap raised ('' when none did). `gap` takes the time and the number of
    arrivals the source has made. With `stop_at` the sources are interrupted then and
    the run goes on until nothing is left; a task takes `service` seconds."""
    env = simpy.Environment()
    trace: Trace = []
    sources: List[simpy.Process] = []

    def task(key: str):
        if service:
            yield env.timeout(service)
        trace.append((env.now, key))
        yield env.timeout(0)

    def source(name: str):
        made = 0
        try:
            while True:
                yield env.timeout(gap(env.now, made))
                made += 1
                env.process(task(f"{name}-{made}"))
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
    error = ""
    try:
        env.run(until=None if stop_at is not None else until)
    except ValueError as exc:
        error = type(exc).__name__
    return trace, error
