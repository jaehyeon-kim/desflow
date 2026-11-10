"""The SimPy program of A1 with several entities: a starter process creates the
entities `stagger` seconds apart, each entity is a process that starts the task
`max_events` times, one gap apart, and the first start waits until `first_at` when it
is given (a timeout, also when that time is past).

A run that stops its arrivals at `stop_at` interrupts the starter and every entity,
as `stop_arrivals` does; the tasks already started finish."""

from typing import Any, List, Optional, Sequence, Tuple

import simpy

Trace = List[Tuple[Any, ...]]


def reference(
    keys: Sequence[str],
    max_events: int,
    gap: float,
    stagger: float = 0,
    first_at: Optional[float] = None,
    stop_at: Optional[float] = None,
    service: float = 0,
) -> Trace:
    """The (time, key) of every start, in the order the tasks finish.

    Args:
        keys: The key of each entity, in the order they are created.
        max_events: How many times each entity starts the task.
        gap: The time before each start after the first.
        stagger: The time between the creation of two entities.
        first_at: The time of each entity's first start, or None for one gap.
        stop_at: When the arrivals stop, or None for never.
        service: The seconds a started task takes; a task is traced when it ends.
    """
    env = simpy.Environment()
    trace: Trace = []
    processes: List[simpy.Process] = []

    def task(key: str):
        if service:
            yield env.timeout(service)
        trace.append((env.now, key))
        yield env.timeout(0)

    def entity(key: str):
        try:
            for index in range(max_events):
                if index == 0 and first_at is not None:
                    yield env.timeout(max(first_at - env.now, 0))
                else:
                    yield env.timeout(gap)
                env.process(task(key))
        except simpy.Interrupt:
            return

    def starter():
        try:
            for index, key in enumerate(keys):
                processes.append(env.process(entity(key)))
                if index < len(keys) - 1 and stagger > 0:
                    yield env.timeout(stagger)
        except simpy.Interrupt:
            return
        yield env.timeout(0)

    def stopper(first: simpy.Process):
        yield env.timeout(stop_at)
        for process in [first, *processes]:
            if process.is_alive:
                process.interrupt()

    begun = env.process(starter())
    if stop_at is not None:
        env.process(stopper(begun))
    env.run()
    return trace
